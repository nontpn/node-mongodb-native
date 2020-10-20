import { Db, DbOptions } from './db';
import { EventEmitter } from 'events';
import { ChangeStream, ChangeStreamOptions } from './change_stream';
import { ReadPreference } from './read_preference';
import { MongoError, AnyError } from './error';
import { WriteConcern } from './write_concern';
import { maybePromise, MongoDBNamespace, Callback } from './utils';
import { deprecate } from 'util';
import { connect, validOptions } from './operations/connect';
import { PromiseProvider } from './promise_provider';
import { Logger } from './logger';
import { ReadConcern } from './read_concern';
import type { Document } from './bson';
import type { Topology } from './sdam/topology';
import type { ClientSession, ClientSessionOptions } from './sessions';
import type { OperationParent } from './operations/command';
import type { MongoClientOptions } from './mongo_client_options';

type CleanUpHandlerFunction = (err?: AnyError, result?: any, opts?: any) => Promise<void>;

/** @public */
export type WithSessionCallback = (session: ClientSession) => Promise<any> | void;

/** @internal */
export interface MongoClientPrivate {
  url: string;
  options?: MongoClientOptions;
  dbCache: Map<string, Db>;
  sessions: Set<ClientSession>;
  readConcern?: ReadConcern;
  writeConcern?: WriteConcern;
  namespace: MongoDBNamespace;
  logger: Logger;
}

/**
 * The **MongoClient** class is a class that allows for making Connections to MongoDB.
 * @public
 *
 * @example
 * ```js
 * // Connect using a MongoClient instance
 * const MongoClient = require('mongodb').MongoClient;
 * const test = require('assert');
 * // Connection url
 * const url = 'mongodb://localhost:27017';
 * // Database Name
 * const dbName = 'test';
 * // Connect using MongoClient
 * const mongoClient = new MongoClient(url);
 * mongoClient.connect(function(err, client) {
 *   const db = client.db(dbName);
 *   client.close();
 * });
 * ```
 *
 * @example
 * ```js
 * // Connect using the MongoClient.connect static method
 * const MongoClient = require('mongodb').MongoClient;
 * const test = require('assert');
 * // Connection url
 * const url = 'mongodb://localhost:27017';
 * // Database Name
 * const dbName = 'test';
 * // Connect using MongoClient
 * MongoClient.connect(url, function(err, client) {
 *   const db = client.db(dbName);
 *   client.close();
 * });
 * ```
 */
export class MongoClient extends EventEmitter implements OperationParent {
  /** @internal */
  s: MongoClientPrivate;
  topology?: Topology;

  constructor(url: string, options?: MongoClientOptions) {
    super();

    if (options && options.promiseLibrary) {
      PromiseProvider.set(options.promiseLibrary);
    }

    // The internal state
    this.s = {
      url,
      options: options || {},
      dbCache: new Map(),
      sessions: new Set(),
      readConcern: ReadConcern.fromOptions(options),
      writeConcern: WriteConcern.fromOptions(options),
      namespace: new MongoDBNamespace('admin'),
      logger: options?.logger ?? new Logger('MongoClient')
    };
  }

  get readConcern(): ReadConcern | undefined {
    return this.s.readConcern;
  }

  get writeConcern(): WriteConcern | undefined {
    return this.s.writeConcern;
  }

  get readPreference(): ReadPreference {
    return ReadPreference.primary;
  }

  get logger(): Logger {
    return this.s.logger;
  }

  /**
   * Connect to MongoDB using a url
   *
   * @see docs.mongodb.org/manual/reference/connection-string/
   */
  connect(): Promise<MongoClient>;
  connect(callback?: Callback<MongoClient>): void;
  connect(callback?: Callback<MongoClient>): Promise<MongoClient> | void {
    if (callback && typeof callback !== 'function') {
      throw new TypeError('`connect` only accepts a callback');
    }

    return maybePromise(callback, cb => {
      const err = validOptions(this.s.options as any);
      if (err) return cb(err);

      connect(this, this.s.url, this.s.options as any, err => {
        if (err) return cb(err);
        cb(undefined, this);
      });
    });
  }

  /**
   * Close the db and its underlying connections
   *
   * @param force - Force close, emitting no events
   * @param callback - An optional callback, a Promise will be returned if none is provided
   */
  close(): Promise<void>;
  close(callback: Callback<void>): void;
  close(force: boolean): Promise<void>;
  close(force: boolean, callback: Callback<void>): void;
  close(
    forceOrCallback?: boolean | Callback<void>,
    callback?: Callback<void>
  ): Promise<void> | void {
    if (typeof forceOrCallback === 'function') {
      callback = forceOrCallback;
    }

    const force = typeof forceOrCallback === 'boolean' ? forceOrCallback : false;

    return maybePromise(callback, cb => {
      if (this.topology == null) {
        return cb();
      }

      const topology = this.topology;
      topology.close({ force }, err => {
        const autoEncrypter = topology.s.options.autoEncrypter;
        if (!autoEncrypter) {
          cb(err);
          return;
        }

        autoEncrypter.teardown(force, err2 => cb(err || err2));
      });
    });
  }

  /**
   * Create a new Db instance sharing the current socket connections.
   * Db instances are cached so performing db('db1') twice will return the same instance.
   * You can control these behaviors with the options noListener and returnNonCachedInstance.
   *
   * @param dbName - The name of the database we want to use. If not provided, use database name from connection string.
   * @param options - Optional settings for Db construction
   */
  db(dbName: string): Db;
  db(dbName: string, options: DbOptions & { returnNonCachedInstance?: boolean }): Db;
  db(dbName: string, options?: DbOptions & { returnNonCachedInstance?: boolean }): Db {
    options = options || {};

    // Default to db from connection string if not provided
    if (!dbName && this.s.options?.dbName) {
      dbName = this.s.options?.dbName;
    }

    // Copy the options and add out internal override of the not shared flag
    const finalOptions = Object.assign({}, this.s.options, options);

    // Do we have the db in the cache already
    const dbFromCache = this.s.dbCache.get(dbName);
    if (dbFromCache && finalOptions.returnNonCachedInstance !== true) {
      return dbFromCache;
    }

    // If no topology throw an error message
    if (!this.topology) {
      throw new MongoError('MongoClient must be connected before calling MongoClient.prototype.db');
    }

    // Return the db object
    const db = new Db(dbName, this.topology, finalOptions);

    // Add the db to the cache
    this.s.dbCache.set(dbName, db);
    // Return the database
    return db;
  }

  /** Check if MongoClient is connected */
  isConnected(): boolean {
    if (!this.topology) return false;
    return this.topology.isConnected();
  }

  /**
   * Connect to MongoDB using a url
   *
   * @see https://docs.mongodb.org/manual/reference/connection-string/
   */
  static connect(url: string): Promise<MongoClient>;
  static connect(url: string, callback: Callback<MongoClient>): void;
  static connect(url: string, options: MongoClientOptions): Promise<MongoClient>;
  static connect(url: string, options: MongoClientOptions, callback: Callback<MongoClient>): void;
  static connect(
    url: string,
    options?: MongoClientOptions | Callback<MongoClient>,
    callback?: Callback<MongoClient>
  ): Promise<MongoClient> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};

    if (options && options.promiseLibrary) {
      PromiseProvider.set(options.promiseLibrary);
    }

    // Create client
    const mongoClient = new MongoClient(url, options);
    // Execute the connect method
    return mongoClient.connect(callback);
  }

  /** Starts a new session on the server */
  startSession(): ClientSession;
  startSession(options: ClientSessionOptions): ClientSession;
  startSession(options?: ClientSessionOptions): ClientSession {
    options = Object.assign({ explicit: true }, options);
    if (!this.topology) {
      throw new MongoError('Must connect to a server before calling this method');
    }

    if (!this.topology.hasSessionSupport()) {
      throw new MongoError('Current topology does not support sessions');
    }

    return this.topology.startSession(options, this.s.options);
  }

  /**
   * Runs a given operation with an implicitly created session. The lifetime of the session
   * will be handled without the need for user interaction.
   *
   * NOTE: presently the operation MUST return a Promise (either explicit or implicitly as an async function)
   *
   * @param options - Optional settings for the command
   * @param callback - An callback to execute with an implicitly created session
   */
  withSession(callback: WithSessionCallback): Promise<void>;
  withSession(options: ClientSessionOptions, callback: WithSessionCallback): Promise<void>;
  withSession(
    optionsOrOperation?: ClientSessionOptions | WithSessionCallback,
    callback?: WithSessionCallback
  ): Promise<void> {
    let options: ClientSessionOptions = optionsOrOperation as ClientSessionOptions;
    if (typeof optionsOrOperation === 'function') {
      callback = optionsOrOperation as WithSessionCallback;
      options = { owner: Symbol() };
    }

    if (callback == null) {
      throw new TypeError('Missing required callback parameter');
    }

    const session = this.startSession(options);
    const Promise = PromiseProvider.get();

    let cleanupHandler: CleanUpHandlerFunction = ((err, result, opts) => {
      // prevent multiple calls to cleanupHandler
      cleanupHandler = () => {
        throw new ReferenceError('cleanupHandler was called too many times');
      };

      opts = Object.assign({ throw: true }, opts);
      session.endSession();

      if (err) {
        if (opts.throw) throw err;
        return Promise.reject(err);
      }
    }) as CleanUpHandlerFunction;

    try {
      const result = callback(session);
      return Promise.resolve(result).then(
        result => cleanupHandler(undefined, result, undefined),
        err => cleanupHandler(err, null, { throw: true })
      );
    } catch (err) {
      return cleanupHandler(err, null, { throw: false }) as Promise<void>;
    }
  }

  /**
   * Create a new Change Stream, watching for new changes (insertions, updates,
   * replacements, deletions, and invalidations) in this cluster. Will ignore all
   * changes to system collections, as well as the local, admin, and config databases.
   *
   * @param pipeline - An array of {@link https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/|aggregation pipeline stages} through which to pass change stream documents. This allows for filtering (using $match) and manipulating the change stream documents.
   * @param options - Optional settings for the command
   */
  watch(): ChangeStream;
  watch(pipeline?: Document[]): ChangeStream;
  watch(pipeline?: Document[], options?: ChangeStreamOptions): ChangeStream {
    pipeline = pipeline || [];
    options = options || {};

    // Allow optionally not specifying a pipeline
    if (!Array.isArray(pipeline)) {
      options = pipeline;
      pipeline = [];
    }

    return new ChangeStream(this, pipeline, options);
  }

  /** Return the mongo client logger */
  getLogger(): Logger {
    return this.s.logger;
  }

  /**
   * @deprecated You cannot logout a MongoClient, you can create a new instance.
   */
  logout = deprecate((options: any, callback: Callback): void => {
    if (typeof options === 'function') (callback = options), (options = {});
    if (typeof callback === 'function') callback(undefined, true);
  }, 'Multiple authentication is prohibited on a connected client, please only authenticate once per MongoClient');
}
