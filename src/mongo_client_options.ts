import { ReadConcern, ReadConcernLevelLike, ReadConcernLike } from './read_concern';
import { WriteConcern, WriteConcernOptions } from './write_concern';
import { ReadPreference, ReadPreferenceMode } from './read_preference';
import type { BSONSerializeOptions, Document } from './bson';
import type { AutoEncryptionOptions } from './deps';
import type { CompressorName } from './cmap/wire_protocol/compression';
import type { AuthMechanismId } from './cmap/auth/defaultAuthProviders';
import type { Logger } from './logger';
import { MongoParseError } from './error';

declare const URL: typeof import('url').URL;

/** @public */
export enum LogLevel {
  error = 'error',
  warn = 'warn',
  info = 'info',
  debug = 'debug'
}

/** @public */
export interface PkFactory {
  createPk(): any; // TODO: when js-bson is typed, function should return some BSON type
}

/** @public */
export interface DriverInfo {
  name?: string;
  version?: string;
  platform?: string;
}

/** @public */
export interface Auth {
  /** The username for auth */
  user?: string;
  /** The password for auth */
  pass?: string;
}

const VALID_AUTH_MECHANISMS = new Set([
  'DEFAULT',
  'PLAIN',
  'GSSAPI',
  'MONGODB-CR',
  'MONGODB-X509',
  'MONGODB-AWS',
  'SCRAM-SHA-1',
  'SCRAM-SHA-256'
]);

/**
 * Describes all possible URI query options for the mongo client
 * @public
 * @see https://docs.mongodb.com/manual/reference/connection-string
 */
export interface MongoURIOptions extends Pick<WriteConcernOptions, 'journal' | 'w' | 'wtimeoutMS'> {
  /** Specifies the name of the replica set, if the mongod is a member of a replica set. */
  replicaSet?: string;
  /** Enables or disables TLS/SSL for the connection. */
  tls?: boolean;
  /** A boolean to enable or disables TLS/SSL for the connection. (The ssl option is equivalent to the tls option.) */
  ssl?: MongoURIOptions['tls'];
  /** Specifies the location of a local .pem file that contains either the client’s TLS/SSL certificate or the client’s TLS/SSL certificate and key. */
  tlsCertificateKeyFile?: string;
  /** Specifies the password to de-crypt the tlsCertificateKeyFile. */
  tlsCertificateKeyFilePassword?: string;
  /** Specifies the location of a local .pem file that contains the root certificate chain from the Certificate Authority. This file is used to validate the certificate presented by the mongod/mongos instance. */
  tlsCAFile?: string;
  /** Bypasses validation of the certificates presented by the mongod/mongos instance */
  tlsAllowInvalidCertificates?: boolean;
  /** Disables hostname validation of the certificate presented by the mongod/mongos instance. */
  tlsAllowInvalidHostnames?: boolean;
  /** Disables various certificate validations. */
  tlsInsecure?: boolean;
  /** The time in milliseconds to attempt a connection before timing out. */
  connectTimeoutMS?: number;
  /** The time in milliseconds to attempt a send or receive on a socket before the attempt times out. */
  socketTimeoutMS?: number;
  /** Comma-delimited string of compressors to enable network compression for communication between this client and a mongod/mongos instance. */
  compressors?: string;
  /** An integer that specifies the compression level if using zlib for network compression. */
  zlibCompressionLevel?: number;
  /** The maximum number of connections in the connection pool. */
  maxPoolSize?: number;
  /** The minimum number of connections in the connection pool. */
  minPoolSize?: number;
  /** The maximum number of milliseconds that a connection can remain idle in the pool before being removed and closed. */
  maxIdleTimeMS?: number;
  /** A number that the driver multiples the maxPoolSize value to, to provide the maximum number of threads allowed to wait for a connection to become available from the pool. */
  waitQueueMultiple?: number;
  /** The maximum time in milliseconds that a thread can wait for a connection to become available. */
  waitQueueTimeoutMS?: number;
  /** The level of isolation */
  readConcernLevel?: ReadConcernLevelLike;
  /** Specifies the read preferences for this connection */
  readPreference?: ReadPreferenceMode | ReadPreference;
  /** Specifies, in seconds, how stale a secondary can be before the client stops using it for read operations. */
  maxStalenessSeconds?: number;
  /** Specifies the tags document as a comma-separated list of colon-separated key-value pairs.  */
  readPreferenceTags?: string;
  /** Specify the database name associated with the user’s credentials. */
  authSource?: string;
  /** Specify the authentication mechanism that MongoDB will use to authenticate the connection. */
  authMechanism?: AuthMechanismId;
  /** Specify properties for the specified authMechanism as a comma-separated list of colon-separated key-value pairs. */
  authMechanismProperties?: {
    SERVICE_NAME?: string;
    CANONICALIZE_HOST_NAME?: boolean;
    SERVICE_REALM?: string;
  };
  /** Set the Kerberos service name when connecting to Kerberized MongoDB instances. This value must match the service name set on MongoDB instances to which you are connecting. */
  gssapiServiceName?: string;
  /** The size (in milliseconds) of the latency window for selecting among multiple suitable MongoDB instances. */
  localThresholdMS?: number;
  /** Specifies how long (in milliseconds) to block for server selection before throwing an exception.  */
  serverSelectionTimeoutMS?: number;
  /** When true, instructs the driver to scan the MongoDB deployment exactly once after server selection fails and then either select a server or raise an error. When false, the driver blocks and searches for a server up to the serverSelectionTimeoutMS value. */
  serverSelectionTryOnce?: boolean;
  /** heartbeatFrequencyMS controls when the driver checks the state of the MongoDB deployment. Specify the interval (in milliseconds) between checks, counted from the end of the previous check until the beginning of the next one. */
  heartbeatFrequencyMS?: number;
  /** Specify a custom app name. */
  appName?: string;
  /** Enables retryable reads. */
  retryReads?: boolean;
  /** Enable retryable writes. */
  retryWrites?: boolean;
  /** Allow a driver to force a Single topology type with a connection string containing one host */
  directConnection?: boolean;
}

/** @public */
export interface MongoClientOptions
  extends WriteConcernOptions,
    MongoURIOptions,
    BSONSerializeOptions {
  /** The maximum number of connections in the connection pool. */
  poolSize?: MongoURIOptions['maxPoolSize'];
  /** Validate mongod server certificate against Certificate Authority */
  sslValidate?: boolean;
  /** SSL Certificate store binary buffer. */
  sslCA?: Buffer;
  /** SSL Certificate binary buffer. */
  sslCert?: Buffer;
  /** SSL Key file binary buffer. */
  sslKey?: Buffer;
  /** SSL Certificate pass phrase. */
  sslPass?: string;
  /** SSL Certificate revocation list binary buffer. */
  sslCRL?: Buffer;
  /** Ensure we check server identify during SSL, set to false to disable checking. */
  checkServerIdentity?: false | ((hostname: string, cert: Document) => Error | undefined);
  /** TCP Connection no delay */
  noDelay?: boolean;
  /** TCP Connection keep alive enabled */
  keepAlive?: boolean;
  /** The number of milliseconds to wait before initiating keepAlive on the TCP socket */
  keepAliveInitialDelay?: number;
  /** Version of IP stack. Can be 4, 6 or null (default). If null, will attempt to connect with IPv6, and will fall back to IPv4 on failure */
  family?: 4 | 6 | null;
  /** Server attempt to reconnect #times */
  reconnectTries?: number;
  /** Server will wait number of milliseconds between retries */
  reconnectInterval?: number;
  /** Control if high availability monitoring runs for Replicaset or Mongos proxies */
  ha?: boolean;
  /** The High availability period for replicaset inquiry */
  haInterval?: number;
  /** Force server to assign `_id` values instead of driver */
  forceServerObjectId?: boolean;
  /** Return document results as raw BSON buffers */
  raw?: boolean;
  /** A primary key factory function for generation of custom `_id` keys */
  pkFactory?: PkFactory;
  /** A Promise library class the application wishes to use such as Bluebird, must be ES6 compatible */
  promiseLibrary?: any;
  /** Specify a read concern for the collection (only MongoDB 3.2 or higher supported) */
  readConcern?: ReadConcernLike;
  /** The logging level */
  loggerLevel?: LogLevel;
  /** Custom logger object */
  logger?: Logger;
  /** Enable the wrapping of the callback in the current domain, disabled by default to avoid perf hit */
  domainsEnabled?: boolean;
  /** Validate MongoClient passed in options for correctness */
  validateOptions?: boolean;
  /** The name of the application that created this MongoClient instance. MongoDB 3.4 and newer will print this value in the server log upon establishing each connection. It is also recorded in the slow query log and profile collections */
  appname?: MongoURIOptions['appName'];
  /** The auth settings for when connection to server. */
  auth?: Auth;
  /** Type of compression to use?: snappy or zlib */
  compression?: CompressorName;
  /** The number of retries for a tailable cursor */
  numberOfRetries?: number;
  /** Enable command monitoring for this client */
  monitorCommands?: boolean;
  /** If present, the connection pool will be initialized with minSize connections, and will never dip below minSize connections */
  minSize?: number;
  /** Determines whether or not to use the new url parser. Enables the new, spec-compliant, url parser shipped in the core driver. This url parser fixes a number of problems with the original parser, and aims to outright replace that parser in the near future. Defaults to true, and must be explicitly set to false to use the legacy url parser. */
  useNewUrlParser?: boolean;
  /** Enables the new unified topology layer */
  useUnifiedTopology?: boolean;
  /** Optionally enable client side auto encryption */
  autoEncryption?: AutoEncryptionOptions;
  /** Allows a wrapping driver to amend the client metadata generated by the driver to include information about the wrapping driver */
  driverInfo?: DriverInfo;
  /** String containing the server name requested via TLS SNI. */
  servername?: string;
  /** Name of database to connect to */
  dbName?: string;
}

function getBoolean(value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  const valueString = String(value).toLowerCase();
  if (valueString === 'true') return true;
  if (valueString === 't') return true;
  if (valueString === '1') return true;
  if (valueString === 'y') return true;
  if (valueString === 'yes') return true;

  if (valueString === 'false') return false;
  if (valueString === 'f') return false;
  if (valueString === '0') return false;
  if (valueString === '-1') return false;
  if (valueString === 'n') return false;
  if (valueString === 'no') return false;
  throw new TypeError(`Expected stringified boolean value, got: ${value}`);
}

function getInt(value: unknown): number {
  if (typeof value === 'number') return Math.trunc(value);
  const parsedValue = Number.parseInt(String(value), 10);
  if (!Number.isNaN(parsedValue)) return parsedValue;
  throw new TypeError(`Expected stringified int value, got: ${value}`);
}

function getUint(value: unknown): number {
  try {
    return Math.abs(getInt(value));
  } catch {
    throw new TypeError(`Expected stringified Uint value, got: ${value}`);
  }
}

function getDuration(value: unknown): number {
  try {
    return Math.abs(getUint(value));
  } catch {
    throw new TypeError(`Expected stringified Uint duration value, got: ${value}`);
  }
}

function isRecord(value: unknown): value is Record<string, any> {
  return !!value && typeof value === 'object';
}

function collectHostsFrom(uri: string): { normalizedURI: string; hosts: string } {
  /**
   * The following regular expression validates a connection string and breaks the
   * provide string into the following capture groups: [protocol, username, password, hosts]
   */
  const HOSTS_RX = new RegExp(
    '(?<protocol>mongodb(?:\\+srv|)):\\/\\/(?:(?<username>[^:]*)(?::(?<password>[^@]*))?@)?(?<hosts>[^\\/?]*)(?<rest>.*)',
    'i' // case insensitive
  );
  const match = uri.match(HOSTS_RX);
  if (!match) {
    throw new MongoParseError('Invalid connection string');
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-expect-error
  const { protocol, username, password, hosts, rest } = match.groups;
  if (!protocol || !hosts) {
    throw new MongoParseError('Invalid connection string, protocol and host(s) required');
  }
  const authString = `${username ? `${password ? `${username}:${password}` : username}` : ''}`;
  return {
    normalizedURI: `${protocol.toLowerCase()}://${authString}@nodeInternalDummyHostDoNotUse${rest}`,
    hosts
  };
}

const defaultOptions = new Map<string, unknown>([
  ['authMechanism', 'DEFAULT'],
  ['dbName', 'test'],
  ['socketTimeoutMS', 0],
  ['readPreference', ReadPreference.primary]
]);

/** @internal */
export class MongoOptions {
  appName: string | undefined;
  auth!: Auth;
  authMechanism!:
    | 'DEFAULT'
    | 'PLAIN'
    | 'GSSAPI'
    | 'MONGODB-CR'
    | 'MONGODB-X509'
    | 'MONGODB-AWS'
    | 'SCRAM-SHA-1'
    | 'SCRAM-SHA-256';
  authMechanismProperties: Record<string, any> | undefined;
  autoEncryption: any;
  checkKeys!: boolean;
  checkServerIdentity!: false | ((hostname: string, cert: Document) => Error | undefined);
  compression!: CompressorName;
  compressors: any;
  connectTimeoutMS: any;
  dbName: any;
  directConnection: any;
  domainsEnabled: any;
  driverInfo!: DriverInfo;
  family: any;
  fieldsAsRaw: any;
  forceServerObjectId: any;
  gssapiServiceName: any;
  ha: any;
  haInterval: any;
  heartbeatFrequencyMS: any;
  ignoreUndefined: any;
  keepAlive: any;
  keepAliveInitialDelay: any;
  localThresholdMS: any;
  logger: any;
  loggerLevel: any;
  maxIdleTimeMS: any;
  maxPoolSize: any;
  maxStalenessSeconds: any;
  minInternalBufferSize: any;
  minPoolSize: any;
  minSize: any;
  monitorCommands: any;
  noDelay: any;
  numberOfRetries: any;
  pkFactory!: PkFactory;
  poolSize!: number;
  promiseLibrary: any;
  promoteBuffers!: boolean;
  promoteLongs!: boolean;
  promoteValues!: boolean;
  raw: any;
  readConcern!: ReadConcern;
  readPreference!: ReadPreference;
  readPreferenceTags: any;
  reconnectInterval: any;
  reconnectTries: any;
  replicaSet!: string;
  retryReads: any;
  retryWrites: any;
  serializeFunctions: any;
  servername: any;
  serverSelectionTimeoutMS: any;
  serverSelectionTryOnce: any;
  socketTimeoutMS: any;
  ssl: any;
  sslCA: any;
  sslCert: any;
  sslCRL: any;
  sslKey: any;
  sslPass: any;
  sslValidate: any;
  tls: any;
  tlsAllowInvalidCertificates: any;
  tlsAllowInvalidHostnames: any;
  tlsCAFile: any;
  tlsCertificateKeyFile: any;
  tlsCertificateKeyFilePassword: any;
  tlsInsecure: any;
  useNewUrlParser: any;
  useUnifiedTopology: any;
  validateOptions: any;
  waitQueueMultiple: any;
  waitQueueTimeoutMS: any;
  writeConcern!: WriteConcern;
  zlibCompressionLevel!: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | undefined;

  constructor(uri?: string, options: MongoClientOptions = {}) {
    const { normalizedURI, hosts } = collectHostsFrom(uri ?? 'mongodb://localhost:27017/test');
    const url = new URL(normalizedURI);

    const urlOptions = new Map();
    for (const key of url.searchParams.keys()) {
      const loweredKey = key.toLowerCase();
      if (urlOptions.has(loweredKey)) {
        urlOptions.set(loweredKey, [
          ...urlOptions.get(loweredKey),
          ...url.searchParams.getAll(key)
        ]);
      } else {
        urlOptions.set(loweredKey, url.searchParams.getAll(key));
      }
    }

    const objectOptions = new Map(
      Object.entries(options).map(([k, v]) => [k.toLowerCase(), v] as [string, any])
    );

    const allOptions = new Map();

    const allKeys = new Set([
      ...urlOptions.keys(),
      ...objectOptions.keys(),
      ...defaultOptions.keys()
    ]);
    for (const key of allKeys) {
      const value = [];
      let source;
      if (urlOptions.has(key)) {
        value.push(...urlOptions.get(key));
      }
      if (objectOptions.has(key)) {
        value.push(objectOptions.get(key));
      }
      if (defaultOptions.has(key)) {
        value.push(defaultOptions.get(key));
      }
      allOptions.set(key, { value, source });
    }

    for (const [key, inputValue] of allOptions.entries()) {
      const optionName = key.toLowerCase() as keyof typeof transforms;
      if (optionName in transforms) {
        transforms[optionName].bind(this)(inputValue);
      } else {
        console.warn(`Unsupported option ${optionName}`);
      }
    }
  }

  toJSON(): Record<string, any> {
    const props = { ...this };
    Reflect.set(props, 'uri', this.toURL());
    return JSON.parse(JSON.stringify(props));
  }

  toURL(): string {
    return `mongodb://localhost:21017/?${Object.entries({ ...this })
      .map(([key, value]) => `${key}=${encodeURIComponent(JSON.stringify(value))}`)
      .join('&')}`;
  }
}

const transforms = {
  appname(this: MongoOptions, [value]: unknown[]): void {
    this.appName = value ? `${value}` : undefined;
  },
  auth(this: MongoOptions, [value]: unknown[]): void {
    if (isRecord(value) && typeof value.user === 'string' && typeof value.pass === 'string') {
      this.auth = value;
      return;
    }
    throw new TypeError(`Option 'auth' must be an object with 'user' and 'pass' properties.`);
  },
  authmechanism(this: MongoOptions, [value]: unknown[]): void {
    if (!VALID_AUTH_MECHANISMS.has(String(value))) {
      throw new TypeError(
        `Option 'authMechanism' must be one of ${[...VALID_AUTH_MECHANISMS].sort()} got ${value}.`
      );
    }
    this.authMechanism = String(value) as AuthMechanismId;
  },
  authmechanismproperties(this: MongoOptions, [value]: unknown[]): void {
    if (!isRecord(value)) {
      throw new TypeError(`Option authMechanismProperties must be an object got ${value}.`);
    }
    this.authMechanismProperties = value;
  },
  authsource(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Option 'authSource' is deprecated use dbName.`);
    this.dbName = String(value);
  },
  autoencryption(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented autoEncryption option. ${value}`);
    this.autoEncryption;
  },
  checkkeys(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented checkKeys option. ${value}`);
    this.checkKeys = getBoolean(value);
  },
  checkserveridentity(this: MongoOptions, [value]: unknown[]): void {
    if (!(typeof value === 'function')) {
      throw new TypeError(`Option 'checkServerIdentity' must be a function`);
    }
    this.checkServerIdentity = value as any;
  },
  compression(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented compression option. ${value}`);
    const compressors = ['none', 'zlib', 'snappy'];
    if (!compressors.includes(String(value))) {
      throw new TypeError(`Option 'compression' must be one of ${compressors}`);
    }
    this.compression = String(value) as CompressorName;
  },
  compressors(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented compressors option. ${value}`);
    this.compressors;
  },
  connecttimeoutms(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented connectTimeoutMS option. ${value}`);
    this.connectTimeoutMS;
  },
  dbname(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented dbName option. ${value}`);
    this.dbName;
  },
  directconnection(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented directConnection option. ${value}`);
    this.directConnection;
  },
  domainsenabled(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented domainsEnabled option. ${value}`);
    this.domainsEnabled;
  },
  driverinfo(this: MongoOptions, [value]: unknown[]): void {
    if (!isRecord(value)) {
      throw new TypeError(`Option 'driverInfo' must be an object got ${typeof value}.`);
    }
    this.driverInfo = value;
  },
  family(this: MongoOptions, [value]: unknown[]): void {
    const valueInt = getInt(value);
    if (![4, 6].includes(valueInt)) {
      throw new TypeError(`Option 'family' must be 4 or 6 got ${valueInt}.`);
    }
    this.family = valueInt;
  },
  fieldsasraw(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented fieldsAsRaw  option. ${value}`);
    this.fieldsAsRaw = getBoolean(value);
  },
  forceserverobjectid(this: MongoOptions, [value]: unknown[]): void {
    this.forceServerObjectId = getBoolean(value);
  },
  fsync(this: MongoOptions, [value]: unknown[]): void {
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    this.writeConcern.fsync = getBoolean(value);
  },
  gssapiservicename(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented gssapiServiceName option. ${value}`);
    this.gssapiServiceName;
  },
  ha(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented ha option. ${value}`);
    this.ha;
  },
  hainterval(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented haInterval option. ${value}`);
    this.haInterval;
  },
  heartbeatfrequencyms(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented heartbeatFrequencyMS option. ${value}`);
    this.heartbeatFrequencyMS;
  },
  ignoreundefined(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented ignoreUndefined option. ${value}`);
    this.ignoreUndefined;
  },
  j(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Option 'j' is deprecated please use journal. ${value}`);
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    this.writeConcern.j = getBoolean(value);
  },
  journal(this: MongoOptions, [value]: unknown[]): void {
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    this.writeConcern.j = getBoolean(value);
  },
  keepalive(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented keepAlive option. ${value}`);
    this.keepAlive;
  },
  keepaliveinitialdelay(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented keepAliveInitialDelay option. ${value}`);
    this.keepAliveInitialDelay;
  },
  localthresholdms(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented localThresholdMS option. ${value}`);
    this.localThresholdMS;
  },
  logger(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented logger option. ${value}`);
    this.logger;
  },
  loggerlevel(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented loggerLevel option. ${value}`);
    this.loggerLevel;
  },
  maxidletimems(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented maxIdleTimeMS option. ${value}`);
    this.maxIdleTimeMS;
  },
  maxpoolsize(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented maxPoolSize option. ${value}`);
    this.maxPoolSize;
  },
  maxstalenessseconds(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented maxStalenessSeconds option. ${value}`);
    this.maxStalenessSeconds;
  },
  mininternalbuffersize(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented minInternalBufferSize option. ${value}`);
    this.minInternalBufferSize;
  },
  minpoolsize(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented minPoolSize option. ${value}`);
    this.minPoolSize;
  },
  minsize(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented minSize option. ${value}`);
    this.minSize;
  },
  monitorcommands(this: MongoOptions, [value]: unknown[]): void {
    this.monitorCommands = getBoolean(value);
  },
  nodelay(this: MongoOptions, [value]: unknown[]): void {
    this.noDelay = getBoolean(value);
  },
  numberofretries(this: MongoOptions, [value]: unknown[]): void {
    this.numberOfRetries = getUint(value);
  },
  pkfactory(this: MongoOptions, [value]: unknown[]): void {
    if (isRecord(value) && 'createPk' in value && typeof value.createPk === 'function') {
      this.pkFactory = value as PkFactory;
      return;
    }
    throw new TypeError(
      `Option pkFactory must be an object with a createPk function, got ${value}`
    );
  },
  poolsize(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented poolSize option. ${value}`);
    this.poolSize;
  },
  promiselibrary(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented promiseLibrary option. ${value}`);
    this.promiseLibrary;
  },
  promotebuffers(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented promoteBuffers option. ${value}`);
    this.promoteBuffers;
  },
  promotelongs(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented promoteLongs option. ${value}`);
    this.promoteLongs;
  },
  promotevalues(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented promoteValues option. ${value}`);
    this.promoteValues;
  },
  raw(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented raw option. ${value}`);
    this.raw;
  },
  readconcern(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented readConcern option. ${value}`);
    this.readConcern;
  },
  readconcernlevel(this: MongoOptions, [value]: unknown[]): void {
    const readConcernLevels = ['local', 'majority', 'linearizable', 'available', 'snapshot'];
    if (!readConcernLevels.includes(String(value))) {
      throw new TypeError(`Option 'readConcernLevel' must be one of ${readConcernLevels}`);
    }
    if (!this.readConcern) {
      this.readConcern = new ReadConcern(String(value) as ReadConcernLevelLike);
    }
    this.readConcern.level = String(value);
  },
  readpreference(this: MongoOptions, [value]: unknown[]): void {
    if (value instanceof ReadPreference) {
      this.readPreference = value;
      return;
    }
    if (typeof value === 'string') {
      this.readPreference = ReadPreference.fromString(value);
      return;
    }
    if (isRecord(value)) {
      this.readPreference = ReadPreference.fromOptions(value) ?? ReadPreference.primary;
      return;
    }

    throw new TypeError(
      `Option readPreference must be a string, plain object, or ReadPreference got ${JSON.stringify(
        value
      )}.`
    );
  },
  //
  readpreferencetags(this: MongoOptions, values: unknown[]): void {
    if (!this.readPreferenceTags) {
      this.readPreferenceTags = [];
    }
    for (const readPreferenceTag of values) {
      if (typeof readPreferenceTag === 'string') {
        const tagSet = readPreferenceTag.split(',');
        for (const tag of tagSet) {
          const [key, value] = tag.split(':');
          this.readPreferenceTags.push({ [key]: value });
        }
      } else if (typeof readPreferenceTag === 'object' && readPreferenceTag) {
        if (Array.isArray(readPreferenceTag)) {
          for (const tag of readPreferenceTag) {
            this.readPreferenceTags.push(tag);
          }
        } else {
          this.readPreferenceTags.push(readPreferenceTag);
        }
      }
    }
    // TODO: remove duplicates?
  },
  reconnectinterval(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented reconnectInterval option. ${value}`);
    this.reconnectInterval;
  },
  reconnecttries(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented reconnectTries option. ${value}`);
    this.reconnectTries;
  },
  replicaset(this: MongoOptions, [value]: unknown[]): void {
    this.replicaSet = String(value);
  },
  retryreads(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented retryReads option. ${value}`);
    this.retryReads = getBoolean(value);
  },
  retrywrites(this: MongoOptions, [value]: unknown[]): void {
    this.retryWrites = getBoolean(value);
  },
  serializefunctions(this: MongoOptions, [value]: unknown[]): void {
    this.serializeFunctions = getBoolean(value);
  },
  servername(this: MongoOptions, [value]: unknown[]): void {
    this.servername = String(value);
  },
  serverselectiontimeoutms(this: MongoOptions, [value]: unknown[]): void {
    this.serverSelectionTimeoutMS = getDuration(value);
  },
  serverselectiontryonce(this: MongoOptions, [value]: unknown[]): void {
    this.serverSelectionTryOnce = getBoolean(value);
  },
  sockettimeoutms(this: MongoOptions, [value]: unknown[]): void {
    this.socketTimeoutMS = getDuration(value);
  },
  ssl(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented ssl option. ${value}`);
    this.ssl;
  },
  sslca(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslCA option. ${value}`);
    this.sslCA;
  },
  sslcert(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslCert option. ${value}`);
    this.sslCert;
  },
  sslcrl(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslCRL option. ${value}`);
    this.sslCRL;
  },
  sslkey(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslKey option. ${value}`);
    this.sslKey;
  },
  sslpass(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslPass option. ${value}`);
    this.sslPass;
  },
  sslvalidate(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented sslValidate option. ${value}`);
    this.sslValidate;
  },
  tls(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tls option. ${value}`);
    this.tls;
  },
  tlsallowinvalidcertificates(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsAllowInvalidCertificates option. ${value}`);
    this.tlsAllowInvalidCertificates;
  },
  tlsallowinvalidhostnames(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsAllowInvalidHostnames option. ${value}`);
    this.tlsAllowInvalidHostnames;
  },
  tlscafile(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsCAFile option. ${value}`);
    this.tlsCAFile;
  },
  tlscertificatekeyfile(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsCertificateKeyFile option. ${value}`);
    this.tlsCertificateKeyFile;
  },
  tlscertificatekeyfilepassword(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsCertificateKeyFilePassword option. ${value}`);
    this.tlsCertificateKeyFilePassword;
  },
  tlsinsecure(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented tlsInsecure option. ${value}`);
    this.tlsInsecure;
  },
  validateoptions(this: MongoOptions): void {
    this.validateOptions = true; // this isn't an expensive operation...
  },
  w(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented w option. ${value}`);
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    const valueInt = Number.parseInt(String(value), 10);
    if (Number.isNaN(valueInt)) {
      this.writeConcern.w = String(value);
    } else {
      this.writeConcern.w = valueInt;
    }
  },
  waitqueuemultiple(this: MongoOptions, [value]: unknown[]): void {
    console.warn(`Unimplemented waitQueueMultiple option. ${value}`);
    this.waitQueueMultiple;
  },
  waitqueuetimeoutms(this: MongoOptions, [value]: unknown[]): void {
    this.waitQueueTimeoutMS = Number.parseInt(String(value), 10);
  },
  writeconcern(this: MongoOptions, [value]: unknown[]): void {
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    if (value instanceof WriteConcern) {
      //
    }
    if (typeof value === 'string' && !Number.isInteger(Number(value))) {
      this.writeConcern.w = value;
    }
    if (typeof value === 'string' && Number.isInteger(Number(value))) {
      this.writeConcern.w = Number(value);
    }
    if (isRecord(value)) {
      if ('w' in value) {
        this.writeConcern.w = value.w;
      }
      if ('j' in value) {
        this.writeConcern.j = getBoolean(value.j);
      }
      if ('wtimeout' in value) {
        this.writeConcern.wtimeout = getDuration(value.wtimeout);
      }
      if ('fsync' in value) {
        this.writeConcern.fsync = getBoolean(value.fsync);
      }
    }
  },
  wtimeout(this: MongoOptions, [value]: unknown[]): void {
    console.warn('wtimeout is deprecated, Please use wtimeoutMS.');
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    this.writeConcern.wtimeout = getDuration(value);
  },
  wtimeoutms(this: MongoOptions, [value]: unknown[]): void {
    if (!this.writeConcern) {
      this.writeConcern = new WriteConcern();
    }
    this.writeConcern.wtimeout = getDuration(value);
  },
  zlibcompressionlevel(this: MongoOptions, [value]: unknown[][]): void {
    const level = Number(value[0]);
    this.zlibCompressionLevel = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].includes(level)
      ? (level as typeof this.zlibCompressionLevel)
      : undefined;
  }
} as const;
