const MilestoneDB = require('sharedb').MilestoneDB;
const mongodb = require('mongodb');

class MongoMilestoneDB extends MilestoneDB {
  constructor(mongo, options) {
    if (typeof mongo === 'object') {
      options = mongo;
      mongo = options.mongo;
    }

    options = options || {};
    super(options);

    this._disableIndexCreation = options.disableIndexCreation;
    this._milestoneIndexes = new Set();

    this._mongo = MongoMilestoneDB._connect(mongo, options);
  }

  async close(callback) {
    if (!callback) callback = () => {};
    let error;
    try { await this._close(); } catch (e) { error = e; }
    process.nextTick(callback, error);
  }

  async saveMilestoneSnapshot(collectionName, snapshot, callback) {
    let wasSaved = false;

    if (!callback) {
      callback = (error) => {
        error ? this.emit('error', error) : this.emit('save', wasSaved, collectionName, snapshot);
      };
    }

    if (!snapshot) return process.nextTick(callback, null, wasSaved, collectionName, snapshot);

    try {
      await this._saveMilestoneSnapshot(collectionName, snapshot);
      wasSaved = true;
    } catch (error) {
      return process.nextTick(callback, error);
    }

    return process.nextTick(callback, null, wasSaved);
  }

  async getMilestoneSnapshot(collectionName, id, version, callback) {
    let snapshot;
    let error;
    try {
      snapshot = await this._getMilestoneSnapshot(collectionName, id, version);
    } catch (e) { error = e; }
    process.nextTick(callback, error, snapshot);
  }

  async _saveMilestoneSnapshot(collectionName, snapshot) {
    const collection = await this._collection(collectionName);

    const query = { id: snapshot.id, v: snapshot.v };
    const doc = MongoMilestoneDB._shallowClone(snapshot);
    const options = { upsert: true };

    return collection.updateOne(query, doc, options);
  }

  async _getMilestoneSnapshot(collectionName, id, version) {
    const collection = await this._collection(collectionName);

    const query = { id: id };
    if (version !== null) {
      query.v = { $lte: version };
    }

    const snapshot = await collection
      .find(query)
      .project({ _id: 0 })
      .sort({ v: -1 })
      .limit(1)
      .next();

    if (snapshot) {
      snapshot.m = snapshot.m == null ? null : snapshot.m;
    }

    return snapshot || undefined;
  }

  async _close() {
    const db = await this._db();
    this._mongo = null;
    return db.close();
  }

  async _db() {
    if (!this._mongo) throw new MongoClosedError();
    return this._mongo;
  }

  async _collection(collectionName) {
    const db = await this._db();
    const name = MongoMilestoneDB._milestoneCollectionName(collectionName);
    const collection = db.collection(name);

    if (this._shouldCreateIndex(name)) {
      // WARNING: Creating indexes automatically like this is quite dangerous in
      // production if we are starting with a lot of data and no indexes
      // already. If new indexes were added or definition of these indexes were
      // changed, users upgrading this module could unsuspectingly lock up their
      // databases. If indexes are created as the first ops are added to a
      // collection this won't be a problem, but this is a dangerous mechanism.
      // Perhaps we should only warn instead of creating the indexes, especially
      // when there is a lot of data in the collection.
      await collection.createIndex({ id: 1, v: 1 }, { background: true, unique: true });
      this._milestoneIndexes.add(name);
    }

    return collection;
  }

  _shouldCreateIndex(milestoneCollectionName) {
    // Given the potential problems with creating indexes on the fly, it might
    // be preferable to disable automatic creation
    return !this._disableIndexCreation
      && !this._milestoneIndexes.has(milestoneCollectionName);
  }

  static _milestoneCollectionName(collectionName) {
    return `m_${ collectionName }`;
  }

  static async _connect(mongo, options) {
    if (typeof mongo === 'function') {
      return new Promise((resolve, reject) => {
        mongo((error, db) => {
          error ? reject(error) : resolve(db);
        });
      });
    }

    return mongodb.connect(mongo, options);
  }

  static _shallowClone(object) {
    const out = {};
    for (const key in object) {
      out[key] = object[key];
    }
    return out;
  }
}

class MongoClosedError extends Error {
  constructor() {
    super();
    this.code = 5105;
    this.message = 'Already closed';
  }
}

module.exports = MongoMilestoneDB;
