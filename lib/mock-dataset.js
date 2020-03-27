/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * TODO:
 * - Handle mounts and unmounts so that things come and go from a mock
 *   filesystem namespace.
 * - Proper Errors rather than assert().
 */
const assert = require('assert-plus');
const path = require('path');
const sprintf = require('sprintf-js').sprintf;
const deepcopy = require('deepcopy');
const VError = require('verror').VError;

var fsver = 5;

// See Dataset.reset().
var txg;
var pending_txg;
var pools;

class Dataset {
    /**
     * Create a new dataset (filesystem, volume, or snapshot).
     * @param {Dataset|null} parent - The parent Dataset or null if creating a
     * new pool.
     * @param {string} name - The name of the new dataset, relative to
     * parent.name.  That is, name must not have '/' or '@' in it.
     * @param {string} type - One of 'filesystem', 'volume', or 'snapshot'.
     * @param {Object} properties - ZFS propeties to set on this dataset.
     */
    constructor(parent, name, type, props) {
        var self = this;
        props = props || {};

        self._parent = parent || pools;
        Dataset.namecheck(name);

        if (self._parent === pools && type !== 'filesystem') {
            throw new VError({ info: arguments, name: 'DatasetTypeError' },
                'top level dataset must be a filesystem');
        }

        self._name = name;
        self._local = {
            type: type,
            creation: new Date(),
            createtxg: txg,
            version: props.version || fsver,
            guid: Math.floor(Math.random() * Math.pow(2, 64))
        };
        self._data = {};

        var siblings;
        switch (type) {
            case 'filesystem':
                self._sep = '/';
                self._parent_types = new Set([ 'filesystem' ]);
                self._child_types = new Set([ 'snapshot', 'volume' ]);
                self._children = {};
                self._snapshots = {};
                siblings = self._parent._children;
                break;
            case 'volume':
                self._sep = '/';
                self._parent_types = new Set([ 'filesystem' ]);
                self._child_types = new Set([ 'snapshot' ]) ;
                self._snapshots = {};
                self._local.volblocksize = 8192;
                siblings = self._parent._children;
                break;
            case 'snapshot':
                self._sep = '@';
                self._parent_types = new Set([ 'filesystem', 'volume' ]);
                self._child_types = new Set();
                self._holds = new Set();
                self._clones = [];
                siblings = self._parent._snapshots;
                break;
            default:
                throw new VError({ info: arguments, name: 'DatasetTypeError' },
                    `unsupported dataset type '${type}'`);
        }

        /*
         * Set props that were passed in via the setter.  A setter that allows
         * special behavior during creation (e.g. setting volblocksize,
         * encryption) should check self._creating.  Once creation is done, we
         * seal this object so that a caller cannot accidentally set a property
         * that this not supported.
         */
        self._state = 'creating';
        for (var prop in props) {
            self[prop] = props[prop];
        }
        Object.seal(self);
        // A little premature, but allows getters to work from here on.
        self._state = 'active';

        if (self._parent !== pools &&
            !self._parent_types.has(self._parent.type)) {

            throw new VError({ info: arguments, name: 'DatasetTypeError' },
                'type %j must be in %j', type, self._parent_types);
        }

        if (siblings.hasOwnProperty(name)) {
            throw new VError({
                info: {
                    parent: self._parent,
                    newname: name,
                    newtype: type
                },
                name: 'DatasetExistsError'
            }, `'${self.name}' already exists`);
        }

        siblings[name] = self;

        if (pending_txg == 0) {
            txg++;
        }

    }

    /**
     * Given a dataset name or a Dataset, return the pool name.  If a name is
     * given, the return value says nothing about the existence of that pool.
     * @param {String|Dataset} ds - The name of a dataset or a dataset.
     * @return string - The name of a pool that may or may not exist.
     */
    static poolname(ds) {
        if (typeof ds === 'string') {
            ds = ds.split(/[@\/]/)[0];
            return ds;
        }
        while (ds._parent !== pools) {
            ds = ds._parent;
        }
        return ds._name;
    }

    /**
     * Find a dataset by name.  For example, Dataset.get('data/foo@blah').
     * @returns {Dataset|null}
     */
    static get(fullname) {
        var name, snapname;
        [ name, snapname ] = fullname.split('@');
        var parts = name.split('/');
        var cur = pools;

        // Look up the filesystem or volume
        for (var i in parts) {
            name = parts[i];
            cur = cur._children[name];
            if (!cur) {
                return null;
            }
        }

        if (snapname) {
            return cur._snapshots[snapname];
        }

        return cur;
    }

    static getPools() {
        return Object.keys(pools._children);
    }

    static destroyPool(poolname) {
        let pool = pools._children[poolname];
        if (!pool) {
            throw new VError({ info: poolname, name: 'NoSuchPoolError' },
                'pool \'%s\' does not exist, pool');
        }

        for (let ds of pool.iterDescendants([ 'all' ])) {
            ds._state = 'pool_destroyed';
        }

        delete pools._children[poolname];
    }

    static namecheck(name) {
        if (typeof name !== 'string') {
            throw new VError({ info: name, name: 'DatasetNameError' },
                'name must be a string');
        }
        // This probably doesn't do unicode and I'm ok with that.
        if (name.length === 0 || name.length > 255) {
            throw new VError({ info: name, name: 'DatasetNameError' },
                'name must be 1 to 255 characters long');
        }
        if (!name.match(/^[a-zA-Z0-9\-_\.: ]+$/)) {
            throw new VError({ info: name, name: 'DatasetNameError' },
                'name may contain only letters numbers, - _ . : and space');
        }
    }

    /**
     * Blow away all the state to start a new test.
     */
    static reset() {
        pools = {
            _children: {},
            // In `zfs get` output these are from the 'default' source.
            _local: {
                atime: 'on',
                canmount: 'on',
                checksum: 'on',
                compression: 'off',
                copies: 1,
                dedup: 'off',
                devices: 'on',
                encryption: 'off',
                exec: 'on',
                keyformat: 'none',
                keylocation: 'none',
                logbias: 'latency',
                mlslabel: 'none',
                mountpoint: '/',
                nbmand: 'off',
                normalization: 'none',
                overlay: 'off',
                primarycache: 'all',
                quota: 'none',
                readonly: 'off',
                recordsize: 128 * 1024,
                redundant_metadata: 'all',
                refquota: 'none',
                refreservation: 'none',
                relatime: 'off',
                reservation: 'none',
                secondarycache: 'all',
                setuid: 'on',
                sharenfs: 'off',
                sharesmb: 'off',
                snapdev: 'hidden',
                snapdir: 'hidden',
                sync: 'standard',
                version: 5,
                volmode: 'default',
                vscan: 'off',
                xattr: 'on',
                zoned: 'off'
            }
        };

        txg = 1;
        pending_txg = 0;
    }

    _assertActive() {
        var self = this;

        if ([ 'active', 'creating' ].indexOf(self._state) === -1) {
            throw new VError({name: 'InactiveDatasetError', info: self},
                'dataset state is \'%s\', not \'active\'', self._state);
        }
    }

    /**
     * Iterate over the children (filesystem, volume, snapshot) and/or
     * dependents (clones of snapshots) of a dataset.
     * @param {(string[]|Set)} types - The types of datasets to iterate. Valid
     * types are 'filesystem', 'volume', 'snapshot', and 'clones'.  'all'
     * implies 'filesystem', 'volume', and 'snapshot', as in `zfs list -r -t
     * all`.  If 'clones' is included, this becomes more like `zfs list -R`
     * @return {Dataset} - Each next() returns a dataset
     */
    * iterDescendants(types, state) {
        this._assertActive();
        var self = this;
        state = state || {};
        state.visited = state.visited || new Set();
        types = new Set(types);
        var do_fs = types.has('all') || types.has('filesystem');
        var do_vol = types.has('all') || types.has('volume');
        var do_snap = types.has('all') || types.has('snapshot');
        var do_clones = types.has('clones');
        var child;

        const oktypes = new Set([
            'all', 'filesystem', 'volume', 'snapshot', 'clones' ]);
        for (let type of types) {
            if (!oktypes.has(type)) {
                throw new VError({ name: 'InvalidArgumentError' },
                    `type '${type}' is not valid`);
            }
        }

        if (!do_fs && !do_vol && !do_snap) {
            throw new VError({info: arguments, name: 'InvalidArgumentError'},
                'iterDescendants() requires dataset type');
        }

        // With 'clones' duplicates are possible if not careful.
        if (state.visited.has(self)) {
            return;
        }
        state.visited.add(self);

        if (types.has('all') || types.has(self.type)) {
            yield self;
        }

        // List snapshots and clones
        if (do_snap || do_clones) {
            for (child in self._snapshots) {
                child = self._snapshots[child];
                yield * child.iterDescendants(types, state);
                if (do_clones) {
                    for (var clone in child._clones) {
                        clone = child._clones[clone];
                        yield * clone.iterDescendants(types, state);
                    }
                }
            }
        }

        // List child filesystems, volumes, and their snapshots.
        for (child in self._children) {
            yield * self._children[child].iterDescendants(types, state);
        }
    }

    /**
     * Iterate over descendants calling checkcb() on each, then docb() on each.
     * If filtercb is specified, it can be used to reduce those that are checked
     * and done.
     * @param {function} checkcb - Called as checkcb(Dataset).  It should throw
     * an error to interrupt iteration if it is not happy with a dataset.
     * @param {function} docb - Called as docb(Dataset).
     * @param {function} [filtercb] - If present, it should return false for
     * those datasets that should not be checked or done.
     */
    _doDescendants(types, checkcb, docb, filtercb) {
        assert.arrayOfString(types, 'types');
        assert.func(checkcb, 'checkcb');
        assert.func(docb, 'docb');
        assert.optionalFunc(filtercb, 'filtercb');
        var self = this;
        var ds;

        for (ds of self.iterDescendants(types)) {
            if (filtercb && !filtercb(ds)) {
                continue;
            }
            checkcb(ds);
        }
        for (ds of self.iterDescendants(types)) {
            if (filtercb && !filtercb(ds)) {
                continue;
            }
            docb(ds);
        }
    }

    mount() {
        // XXX not implemented
    }

    unmount() {
        // XXX not implemented
    }

    /**
     * Destroy this dataset, and perhaps its decsendants.
     * @param {Object} opts
     * @param {boolean} opts.recursive - Destroy descendants (filesystems,
     * volumes, and snapshots living below this dataset in the namespace).  That
     * is, `zfs destroy -r`, not `zfs destroy -R`.
     */
    destroy(opts) {
        this._assertActive();
        var self = this;
        opts = opts || {};
        var recursive = opts.recursive || false;
        var clones = [];

        function check_destroy(check) {
            if (check.type === 'snapshot' && check._holds.size !== 0) {
                throw new VError({ info: this, name: 'SnapshotHoldError' },
                    `snapshot '${check.name}' should have no holds`);
            }
            var kids = check._children;
            var snaps = check._snapshots;
            if (!recursive && ((kids && Object.keys(kids).length !== 0) ||
                (snaps && Object.keys(snaps).length !== 0))) {

                throw new VError({ info: this, name: 'DescendantError' },
                    `dataset '${check.name}' should have no children`);
            }

            for (var clone in check._clones) {
                clones.push(check._clones[clone]);
            }
        }

        // Gather dataset list and sanity check.
        check_destroy(self);
        var todestroy = [];
        if (recursive) {
            for (ds of this.iterDescendants(['all'])) {
                if (ds === self) {
                    continue;
                }
                check_destroy(ds);
                todestroy.push(ds);
            }
        }

        for (var ds in clones) {
            ds = clones[ds];

            if (todestroy.indexOf(ds) === -1) {
                throw new VError({
                    info: this,
                    opts: opts,
                    name: 'DependantError',
                    dataset: ds
                }, `dataset '${ds.name}' requires origin snapshot ` +
                   `'${ds.origin.name}' which would be deleted`);
            }
        }

        // Destroy in reverse order from iteration.  That is, destroy children
        // first.
        while (todestroy.length > 0) {
            todestroy.pop().destroy({recursive: false})
        }

        self.unmount();

        if (self.type === 'snapshot') {
            delete self._parent._snapshots[self._name];
        } else {
            if (self._local.origin) {
                clones = self._local.origin._clones;
                clones.splice(clones.indexOf(self), 1);
            }
            delete self._parent._children[self._name];
        }

        self._state = 'destroyed';
    }

    /**
     * Create a snapshot of a filesystem or hierarchy
     * @param {string} snapname - the name of the snapshot
     * @param {Object} [opts]
     * @param {boolean} [opts.recursive] - if true, be like `zfs snapshot -r`.
     * @param {Object} [properties] - Few properties should be settable on
     * snapshots.  This is probably most useful for user properties.
     * @return {Dataset} the new snapshot dataset
     */
    snapshot(snapname, opts, properties) {
        this._assertActive();
        var self = this;
        assert.optionalObject(opts);
        assert.optionalObject(properties);
        opts = opts || {};
        assert.optionalBool(opts.snapshot);
        properties = properties || {};
        var recursive = opts.recursive || false;
        var errinfo = {
            dataset: self,
            snapname: snapname,
            opts: opts,
            properties: properties
        };

        if (!self._child_types.has('snapshot')) {
            throw new VError({ info: errinfo, name: 'DatasetTypeError' },
                `cannot create snapshot of ${self.type} '${self.name}'`);
        }

        function checksnap(ds) {
            if (ds._snapshots.hasOwnProperty(snapname)) {
                throw new VError({ info: errinfo, name: 'DatasetExistsError' },
                    `'${ds.name}@${snapname}' already exists`);
            }
        }

        function dosnap(ds) {
            var newds = new Dataset(ds, snapname, 'snapshot', properties);
            ds._snapshots[snapname] = newds;
            newds._data = deepcopy(self._data);
        }

        pending_txg = txg;

        try {
            if (recursive) {
                self._doDescendants(['filesystem', 'volume'], checksnap,
                    dosnap);
            } else {
                checksnap(self);
                dosnap(self);
            }
            // XXX need except?
        } finally {
            txg++;
            pending_txg = 0;
        }

        return self._snapshots[snapname];
    }

    clone(newname, opts, properties) {
        this._assertActive();
        var self = this;
        var myname = self.name;
        var poolname = Dataset.poolname(self);
        opts = opts || {};
        properties = properties || {};
        var parents = opts.parents || false;

        if (self.type !== 'snapshot') {
            throw new VError({
                info: {
                    dataset: self,
                    args: arguments
                },
                name: 'DatasetTypeError'
            }, 'can only clone snapshots');
        }

        if (poolname !== Dataset.poolname(newname)) {
            throw new VError({
                info: {
                    dataset: self,
                    args: arguments
                },
                name: 'InvalidArgumentError'
            }, `snapshot '${self.name}' and '${newname}' not in same pool`);
        }
        assert(!newname.startsWith(myname.split('@')[0] = '/'));

        var pname = path.dirname(newname);
        var pds = Dataset.get(pname);
        if (!pds) {
            if (!opts.parents) {
                throw new VError({
                    info: {
                        dataset: self,
                        parent_name: pname,
                        opts: opts,
                        properties: properties
                    },
                    name: 'InvalidArgumentError'
                }, 'parent of \'%s\' must exist', newname);
            }
            var tocreate = [];
            while (!pds && pname !== poolname) {
                pds = Dataset.get(pname)
                tocreate.push(pname);
                pname = path.dirname(pname);
            }
            if (!pds && pname === poolname) {
                pds = Dataset.get(pname);
            }
            assert(pds, 'a parent must exist');
            pname = tocreate.pop();
            while (pname) {
                pds = new Dataset(pds, path.basename(pname), 'filesystem');
                assert(pds, 'a child dataste was created');
                pname = tocreate.pop();
            }
        }

        var newds = new Dataset(pds, path.basename(newname), self._parent.type,
            properties);
        newds._local.origin = self;
        self._clones.push(newds);
        newds._data = deepcopy(self._data);

        return newds;
    }

    hold(reason, opts) {
        this._assertActive();
        var self = this;
        opts = opts || {};
        var recursive = opts.recursive;
        var child, childsnap;

        if (self.type !== 'snapshot') {
            throw new VError({
                info: {
                    dataset: self,
                    args: arguments
                },
                name: 'DatasetTypeError'
            }, 'can only clone snapshots');
        }

        assert(self.type === 'snapshot');
        assert(!self._holds.has(reason));

        function checkhold(_ds) { }

        function addhold(ds) {
            ds._holds.add(reason);
        }

        function filter(ds) {
            return ds._name === self._name;
        }

        if (recursive) {
            self._parent._doDescendants(['snapshot'], checkhold, addhold,
                filter);
            return;
        }

        addhold(self);
    }

    release(reason, opts) {
        this._assertActive();
        var self = this;
        opts = opts || {};
        var recursive = opts.recursive;
        var child, childsnap;

        if (self.type !== 'snapshot') {
            throw new VError({
                info: {
                    dataset: self,
                    args: arguments
                },
                name: 'DatasetTypeError'
            }, 'can only clone snapshots');
        }

        assert(self.type === 'snapshot');
        assert(self._holds.has(reason), `release ${reason} from ${self.name}`);

        function checkhold(_ds) { }

        function rmhold(ds) {
            ds._holds.delete(reason);
        }

        function filter(ds) {
            return ds._name === self._name;
        }

        if (recursive) {
            self._parent._doDescendants(['snapshot'], checkhold, rmhold,
                filter);
            return;
        }

        rmhold(self);
    }

    holds() {
        this._assertActive();
        var self = this;

        if (self.type !== 'snapshot') {
            throw new VError({
                info: {
                    dataset: self,
                    args: arguments
                },
                name: 'DatasetTypeError'
            }, 'can only clone snapshots');
        }

        return new Set(self._holds);
    }

    rename(newname, opts) {
        this._assertActive();
        var self = this;
        opts = opts || {};
        var parents = opts.parents || false;
        var name, snapname, parentname;
        var pds;
        var errinfo = {
            dataset: self,
            newname: snapname,
            opts: opts
        };

        if (Dataset.get(newname)) {
            throw new VError({ info: errinfo, name: 'DatasetExistsError' },
                'cannot rename \'%s\': \'%s\' already exists', self.name,
                newname);
        }

        [ name, snapname ] = newname.split('@');
        if (snapname) {
            if (self.type !== 'snapshot') {
                throw new VError(
                    { info: errinfo, name: 'InvalidArgumentError' },
                    'cannot rename a %s to a snapshot', self.type);
            }
            if (name !== self._parent.name) {
                throw new VError(
                    { info: errinfo, name: 'InvalidArgumentError' },
                    'cannot rename a snapshot to a different parent');
            }
            parentname = name;
            pds = self._parent;
            assert.equal(pds._snapshots[self._name], self,
                'snapshot exists as old name');
            assert(!pds._snapshots[snapname],
                'snapshot does not exist with new name');
            pds._snapshots[snapname] = self;
            delete pds._snapshots[self._name];
            return;
        }

        /* Not a snapshot */

        if (self.type === 'snapshot') {
            throw new VError({ info: errinfo, name: 'InvalidArgumentError' },
                'cannot rename a snapshot to a filesystem or volume',
                self.type);
        }
        if (newname.search('/') === -1) {
            throw new VError({ info: errinfo, name: 'InvalidArgumentError' },
                'new name cannot be a pool name');
        }
        if (Dataset.poolname(self) !== Dataset.poolname(newname)) {
            throw new VError({ info: errinfo, name: 'InvalidArgumentError' },
                'cannot rename \'%s\': new name must be in same pool',
                self.name);
        }

        assert(!parents, 'opts.parents not implemented');
        pds = Dataset.get(path.dirname(newname));

        assert(self._parent._children[self._name] === self,
            'self is a child of parent');
        assert(!pds._children[path.basename(newname)], 'newname is free');
        delete self._parent._children[self._name];
        self._parent = pds;
        self._name = path.basename(newname);
        pds._children[self._name] = self;
    }

    /*
     * getters, setters, and their helpers
     */

    getInheritableValue(propname) {
        this._assertActive();
        var self = this;
        var source;

        var ds = self;
        while (!ds._local.hasOwnProperty(propname)) {
            assert(ds !== pools,
                `pools top-level object should have default for ${propname}`)
            ds = ds._parent;
        }
        switch (ds) {
            case self:
                source = 'local';
                break;
            case pools:
                source = 'default';
                break;
            default:
                source = `inherited from ${ds.name}`;
        }

        return {
            value: ds._local[propname],
            source: source
        };
    }

    get aclinherit() { throw new Error('not implemented'); }
    set aclinherit(_) { throw new Error('not implemented'); }
    get acltype() { throw new Error('not implemented'); }
    set acltype(_) { throw new Error('not implemented'); }

    get atime() {
        this._assertActive();
        return this.getInheritableValue('atime').value;
    }

    set atime(value) {
        this._assertActive();
        assert([ 'on', 'off' ].indexOf(value) !== -1, 'atime is on or off');

        this._local.atime = value;
    }

    get canmount() {
        this._assertActive();
        return this.getInheritableValue('canmount').value;
    }

    set canmount(value) {
        this._assertActive();
        assert([ 'on', 'off', 'noauto' ].indexOf(value) !== -1,
            'canmount is on, off, or noauto');

        this._local.canmount = value;
    }

    get casesensitivity() { throw new Error('not implemented'); }
    set casesensitivity(_) { throw new Error('not implemented'); }

    get checksum() {
        this._assertActive();
        return this.getInheritableValue('checksum').value;
    }

    set checksum(value) {
        this._assertActive();
        var valid = [ 'on', 'off', 'fletcher2', 'fletcher4', 'sha256',
            'noparity', 'sha512', 'skein', 'edonr' ];
        assert(valid.indexOf(value) !== -1,
            sprintf('checksum is one of: %j', valid));

        // XXX:
        // The sha512, skein, and edonr checksum algorithms require enabling the
        // appropriate features on the pool.  These pool features are not
        // supported by GRUB and must not be used on the pool if GRUB needs to
        // access the pool (e.g. for /boot).

        this._local.checksum = value;
    }

    get compression() {
        this._assertActive();
        return this.getInheritableValue('compression').value;
    }

    set compression(value) {
        this._assertActive();
        assert([ 'on', 'off' ].indexOf(value) !== -1,
            'compression is on or off');

        this._local.compression = value;
    }

    get context() { throw new Error('not implemented'); }
    set context(_) { throw new Error('not implemented'); }

    get copies() {
        this._assertActive();
        return this.getInheritableValue('copies').value;
    }

    set copies(value) {
        this._assertActive();
        assert(Math.floor(parseInt(value)) === value,
            'copies must be an integer');
        assert(value >=1 && value <= 3, 'copies must be 1, 2, or 3');

        this._local.copies = value;
    }

    get createtxg() {
        this._assertActive();
        return this._local.createtxg;
    }

    set createtxg(_) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'createtxg'
            },
        }, 'property is read-only');
    }

    get creation() {
        this._assertActive();
        return Math.floor(this._local.creation / 1000);
    }

    set creation(_) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'creation'
            },
        }, 'property is read-only');
    }

    get dedup() { throw new Error('not implemented'); }
    set dedup(_) { throw new Error('not implemented'); }
    get defcontext() { throw new Error('not implemented'); }
    set defcontext(_) { throw new Error('not implemented'); }
    get devices() { throw new Error('not implemented'); }
    set devices(_) { throw new Error('not implemented'); }
    get dnodesize() { throw new Error('not implemented'); }
    set dnodesize(_) { throw new Error('not implemented'); }
    get encryption() { throw new Error('not implemented'); }
    set encryption(_) { throw new Error('not implemented'); }
    get exec() { throw new Error('not implemented'); }
    set exec(_) { throw new Error('not implemented'); }
    get filesystem_count() { throw new Error('not implemented'); }
    set filesystem_count(_) { throw new Error('not implemented'); }
    get filesystem_limit() { throw new Error('not implemented'); }
    set filesystem_limit(_) { throw new Error('not implemented'); }
    get fscontext() { throw new Error('not implemented'); }
    set fscontext(_) { throw new Error('not implemented'); }

    get guid() {
        this._assertActive();
        return this._local.guid;
    }

    set guid(val) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'guid'
            },
        }, 'property is read-only');
    }

    get keyformat() { throw new Error('not implemented'); }
    set keyformat(_) { throw new Error('not implemented'); }
    get keylocation() { throw new Error('not implemented'); }
    set keylocation(_) { throw new Error('not implemented'); }
    get logbias() { throw new Error('not implemented'); }
    set logbias(_) { throw new Error('not implemented'); }
    get logicalreferenced() { throw new Error('not implemented'); }
    set logicalreferenced(_) { throw new Error('not implemented'); }
    get logicalused() { throw new Error('not implemented'); }
    set logicalused(_) { throw new Error('not implemented'); }
    get mlslabel() { throw new Error('not implemented'); }
    set mlslabel(_) { throw new Error('not implemented'); }
    get mounted() { throw new Error('not implemented'); }
    set mounted(_) { throw new Error('not implemented'); }

    // XXX This should be made more generic to work 'zfs get' to support
    // 'inherited from foo/bar'.
    get mountpoint() {
        this._assertActive();
        var self = this;
        if (self.type !== 'filesystem') {
            return null
        }
        var cur = self;
        var trail = [];

        while (cur !== pools && !cur._local.mountpoint) {
            trail.push(cur._name);
            cur = cur._parent;
        }
        if (cur === pools) {
            return '/' + trail.reverse().join('/');
        }
        if (cur._local.mountpoint.startsWith('/')) {
            return [ cur._local.mountpoint ].concat(trail.reverse()).join('/');
        }
        return cur._local.mountpoint;
    }

    set mountpoint(value) {
        this._assertActive();
        var self = this;

        // XXX snapshots of filesystems too?
        assert(self.type === 'filesystem',
            'mountpoint only supported with filesystems');

        assert(value.startsWith('/') ||
            [ 'none', 'legacy'].indexOf(value) !== -1,
            'mountpoint must be \'none\' or \'legacy\' or an absolute path');
        self.unmount();
        self._local.mountpoint = value;
        if (value.startsWith('/') && self.canmount === 'on') {
            self.mount();
        }
    }

    get nmbmand() { throw new Error('not implemented'); }
    set nmbmand(_) { throw new Error('not implemented'); }
    get normalization() { throw new Error('not implemented'); }
    set normalization(_) { throw new Error('not implemented'); }
    get objsetid() { throw new Error('not implemented'); }
    set objsetid(_) { throw new Error('not implemented'); }

    get origin() {
        this._assertActive();
        return this._local.origin;
    }

    set origin(_) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'origin'
            },
        }, 'property is read-only');
    }

    get overlay() { throw new Error('not implemented'); }
    set overlay(_) { throw new Error('not implemented'); }
    get pbkdf2iters() { throw new Error('not implemented'); }
    set pbkdf2iters(_) { throw new Error('not implemented'); }
    get primarycache() { throw new Error('not implemented'); }
    set primarycache(_) { throw new Error('not implemented'); }
    get quota() { throw new Error('not implemented'); }
    set quota(_) { throw new Error('not implemented'); }
    get readonly() { throw new Error('not implemented'); }
    set readonly(_) { throw new Error('not implemented'); }
    get recordize() { throw new Error('not implemented'); }
    set recordize(_) { throw new Error('not implemented'); }
    get redundant_metadata() { throw new Error('not implemented'); }
    set redundant_metadata(_) { throw new Error('not implemented'); }
    get refcompressratio() { throw new Error('not implemented'); }
    set refcompressratio(_) { throw new Error('not implemented'); }
    get referenced() { throw new Error('not implemented'); }
    set referenced(_) { throw new Error('not implemented'); }
    get refquota() { throw new Error('not implemented'); }
    set refquota(_) { throw new Error('not implemented'); }
    get refreservation() { throw new Error('not implemented'); }
    set refreservation(_) { throw new Error('not implemented'); }
    get relatime() { throw new Error('not implemented'); }
    set relatime(_) { throw new Error('not implemented'); }
    get reservation() { throw new Error('not implemented'); }
    set reservation(_) { throw new Error('not implemented'); }
    get rootcontext() { throw new Error('not implemented'); }
    set rootcontext(_) { throw new Error('not implemented'); }
    get secondarycache() { throw new Error('not implemented'); }
    set secondarycache(_) { throw new Error('not implemented'); }
    get setuid() { throw new Error('not implemented'); }
    set setuid(_) { throw new Error('not implemented'); }
    get size() { throw new Error('not implemented'); }
    set size(_) { throw new Error('not implemented'); }
    get sharenfs() { throw new Error('not implemented'); }
    set sharenfs(_) { throw new Error('not implemented'); }
    get sharesmb() { throw new Error('not implemented'); }
    set sharesmb(_) { throw new Error('not implemented'); }
    get snapdev() { throw new Error('not implemented'); }
    set snapdev(_) { throw new Error('not implemented'); }
    get snapdir() { throw new Error('not implemented'); }
    set snapdir(_) { throw new Error('not implemented'); }
    get snapshot_count() { throw new Error('not implemented'); }
    set snapshot_count(_) { throw new Error('not implemented'); }
    get snapshot_limit() { throw new Error('not implemented'); }
    set snapshot_limit(_) { throw new Error('not implemented'); }
    get special_small_blocks() { throw new Error('not implemented'); }
    set special_small_blocks(_) { throw new Error('not implemented'); }
    get sync() { throw new Error('not implemented'); }
    set sync(_) { throw new Error('not implemented'); }

    get type() {
        this._assertActive();
        return this._local.type;
    }

    set type(_) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'type'
            },
        }, 'property is read-only');
    }


    get usedbychildren() { throw new Error('not implemented'); }
    set usedbychildren(_) { throw new Error('not implemented'); }
    get usedbydataset() { throw new Error('not implemented'); }
    set usedbydataset(_) { throw new Error('not implemented'); }
    get usedbyrefreservation() { throw new Error('not implemented'); }
    set usedbyrefreservation(_) { throw new Error('not implemented'); }
    get usedbysnapshots() { throw new Error('not implemented'); }
    set usedbysnapshots(_) { throw new Error('not implemented'); }
    get utf8only() { throw new Error('not implemented'); }
    set utf8only(_) { throw new Error('not implemented'); }
    get version() { throw new Error('not implemented'); }
    set version(_) { throw new Error('not implemented'); }
    get volblocksize() { throw new Error('not implemented'); }
    set volblocksize(_) { throw new Error('not implemented'); }
    get volmode() { throw new Error('not implemented'); }
    set volmode(_) { throw new Error('not implemented'); }
    get vscan() { throw new Error('not implemented'); }
    set vscan(_) { throw new Error('not implemented'); }
    get written() { throw new Error('not implemented'); }
    set written(_) { throw new Error('not implemented'); }
    get xattr() { throw new Error('not implemented'); }
    set xattr(_) { throw new Error('not implemented'); }
    get zoned() { throw new Error('not implemented'); }
    set zoned(_) { throw new Error('not implemented'); }

    get name() {
        this._assertActive();
        if (this._parent !== pools) {
            return path.join(this._parent.name + this._sep + this._name);
        }
        return this._name;
    }

    set name(val) {
        this._assertActive();
        throw new VError({
            name: 'ReadOnlyPropertyError',
            info: {
                dataset: this,
                property: 'name'
            },
        }, 'property is read-only');
    }
}

Dataset.reset();

module.exports = {
    Dataset: Dataset
};
