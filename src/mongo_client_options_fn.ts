import type { ReadPreference } from '.';
import type { Document } from './bson';
import { AuthMechanism, AuthMechanismId } from './cmap/auth/defaultAuthProviders';
import { MongoCredentials } from './cmap/auth/mongo_credentials';
import type { CompressorName } from './cmap/wire_protocol/compression';
import type { AutoEncryptionOptions } from './deps';
import type { Logger } from './logger';
import type { DriverInfo, PkFactory } from './mongo_client_options';
import type { ReadConcern } from './read_concern';
import { WriteConcern } from './write_concern';

function getBoolean(name: string, value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  const valueString = String(value).toLowerCase();
  const truths = ['true', 't', '1', 'y', 'yes'];
  const lies = ['false', 'f', '0', 'n', 'no', '-1'];
  if (truths.includes(valueString)) return true;
  if (lies.includes(valueString)) return false;
  throw new TypeError(`Expected stringified boolean value, got: ${value}`);
}

function getInt(name: string, value: unknown): number {
  if (typeof value === 'number') return Math.trunc(value);
  const parsedValue = Number.parseInt(String(value), 10);
  if (!Number.isNaN(parsedValue)) return parsedValue;
  throw new TypeError(`Expected ${name} to be stringified int value, got: ${value}`);
}

function getUint(name: string, value: unknown): number {
  const parsedValue = getInt(name, value);
  if (parsedValue < 0) {
    throw new TypeError(`${name} can only be a positive int value, got: ${value}`);
  }
  return parsedValue;
}

function isRecord(value: unknown): value is Record<string, any> {
  return !!value && typeof value === 'object';
}

export const OPTIONS: Record<string, OptionDescriptor> = {
  appName: {
    type: 'complex',
    transformName: 'driverInfo',
    transform({ options, values: [value] }): DriverInfo {
      return { ...options.driverInfo, name: String(value) };
    }
  },
  auth: {
    type: 'complex',
    transformName: 'credentials',
    transform({ name, options, values: [value] }): MongoCredentials {
      if (!isRecord(value)) {
        throw new TypeError(`${name} must be an object with 'user' and 'pass' properties`);
      }
      return new MongoCredentials({
        ...options.credentials,
        username: value.user,
        password: value.pass
      });
    }
  },
  authMechanism: {
    type: 'complex',
    transformName: 'credentials',
    transform({ name, options, values: [value] }): MongoCredentials {
      if (Object.values(AuthMechanism).includes(String(value) as AuthMechanismId)) {
        throw new TypeError(`${name} must be an object with user and pass properties`);
      }
      return new MongoCredentials({
        ...options.credentials,
        mechanism: String(value) as AuthMechanismId
      });
    }
  },
  authMechanismProperties: {
    type: 'complex',
    transformName: 'authMechanismProperties',
    transform({ options, values: [value] }): MongoCredentials {
      if (!isRecord(value)) {
        throw new TypeError('AuthMechanismProperties must be an object');
      }
      return new MongoCredentials({ ...options.credentials, mechanismProperties: value });
    }
  },
  authSource: {
    type: 'complex',
    transformName: 'credentials',
    transform({ options, values: [value] }): MongoCredentials {
      return new MongoCredentials({ ...options.credentials, source: String(value) });
    }
  },
  autoEncryption: {
    type: 'record'
  },
  checkKeys: {
    type: 'boolean'
  },
  checkServerIdentity: {
    type: 'complex',
    transformName: 'checkServerIdentity',
    transform({
      values: [value]
    }): boolean | ((hostname: string, cert: Document) => Error | undefined) {
      if (typeof value !== 'boolean' && typeof value !== 'function')
        throw new TypeError('check server identity must be a boolean or custom function');
      return value as boolean | ((hostname: string, cert: Document) => Error | undefined);
    }
  },
  compression: {
    type: 'complex',
    transformName: 'compression',
    transform({ values }): CompressorName[] {
      const compressionList = new Set();
      for (const c of values) {
        if (['none', 'snappy', 'zlib'].includes(String(c))) {
          compressionList.add(String(c));
        } else {
          throw new TypeError(`${c} is not a valid compression mechanism`);
        }
      }
      return [...compressionList] as CompressorName[];
    }
  },
  compressors: {
    type: 'complex',
    transformName: 'compressors',
    transform({ name, values: [value] }): CompressorName {
      const transformed = String(value);
      const validCompressorNames = ['none', 'snappy', 'zlib'];
      if (!validCompressorNames.includes(transformed)) {
        throw new TypeError(`${name} must be one of ${validCompressorNames}`);
      }
      return transformed as CompressorName;
    }
  },
  connectTimeoutMS: {
    type: 'uint'
  },
  createPk: {
    type: 'complex',
    transformName: 'pkFactory',
    transform({ values: [value] }): PkFactory {
      if (typeof value === 'function') {
        return { createPk: value } as PkFactory;
      }
      throw new TypeError(
        `Option pkFactory must be an object with a createPk function, got ${value}`
      );
    }
  },
  dbName: {
    type: 'string'
  },
  directConnection: {
    type: 'boolean'
  },
  domainsEnabled: {
    type: 'boolean'
  },
  driverInfo: {
    type: 'record'
  },
  family: {
    type: 'complex',
    transformName: 'family',
    transform({ name, values: [value] }): 4 | 6 {
      const transformValue = getInt(name, value);
      if (transformValue === 4 || transformValue === 6) {
        return transformValue;
      }
      throw new TypeError(`Option 'family' must be 4 or 6 got ${transformValue}.`);
    }
  },
  fieldsAsRaw: {
    type: 'boolean'
  },
  forceServerObjectId: {
    type: 'boolean'
  },
  fsync: {
    type: 'complex',
    transformName: 'writeConcern',
    transform({ name, options, values: [value] }): WriteConcern {
      const wc = WriteConcern.fromOptions({
        ...options.writeConcern,
        fsync: getBoolean(name, value)
      });
      if (!wc) throw new TypeError(`Unable to make a writeConcern from fsync=${value}`);
      return wc;
    }
  },
  gssapiServiceName: {
    type: 'string'
  },
  ha: {
    type: 'boolean'
  },
  haInterval: {
    type: 'uint'
  },
  heartbeatFrequencyMS: {
    type: 'uint'
  },
  ignoreUndefined: {
    type: 'boolean'
  },
  j: {
    type: 'complex',
    transformName: 'writeConcern',
    transform({ name, options, values: [value] }): WriteConcern {
      console.warn('j is deprecated');
      const wc = WriteConcern.fromOptions({
        ...options.writeConcern,
        journal: getBoolean(name, value)
      });
      if (!wc) throw new TypeError(`Unable to make a writeConcern from journal=${value}`);
      return wc;
    }
  },
  journal: {
    type: 'complex',
    transformName: 'writeConcern',
    transform({ name, options, values: [value] }): WriteConcern {
      const wc = WriteConcern.fromOptions({
        ...options.writeConcern,
        journal: getBoolean(name, value)
      });
      if (!wc) throw new TypeError(`Unable to make a writeConcern from journal=${value}`);
      return wc;
    }
  },
  keepAlive: {
    type: 'boolean'
  },
  keepAliveInitialDelay: {
    type: 'uint'
  },
  localThresholdMS: {
    type: 'uint'
  },
  // logger: {
  //   type: 'complex'
  // },
  // loggerLevel: {
  //   type: 'complex'
  // },
  maxIdleTimeMS: {
    type: 'uint'
  },
  maxPoolSize: {
    type: 'uint'
  },
  maxStalenessSeconds: {
    type: 'uint'
  },
  minInternalBufferSize: {
    type: 'uint'
  },
  minPoolSize: {
    type: 'uint'
  },
  // minSize: {
  //   type: 'complex'
  // },
  monitorCommands: {
    type: 'boolean'
  },
  // name: {
  //   type: 'complex'
  // },
  noDelay: {
    type: 'boolean'
  },
  numberOfRetries: {
    type: 'int'
  },
  // pass: {
  //   type: 'complex',
  //   transform(
  //     name: string,
  //     options: MongoOptions,
  //     [value]: unknown[]
  //   ): TransformResult<MongoCredentials> {
  //     if (!isRecord(value)) {
  //       throw new TypeError('Auth must be an object with user and pass properties');
  //     }
  //     return {
  //       transformName: 'credentials',
  //       transformValue: new MongoCredentials({
  //         username: options.auth.user,
  //         password: String(value)
  //       })
  //     };
  //   }
  // },
  pkFactory: {
    type: 'complex',
    transformName: 'createPk',
    transform({ values: [value] }): PkFactory {
      if (isRecord(value) && 'createPk' in value && typeof value.createPk === 'function') {
        return value as PkFactory;
      }
      throw new TypeError(
        `Option pkFactory must be an object with a createPk function, got ${value}`
      );
    }
  },
  // platform: {
  //   type: 'complex'
  // },
  poolSize: {
    type: 'uint'
  },
  // promiseLibrary: {
  //   type: 'complex'
  // },
  promoteBuffers: {
    type: 'boolean'
  },
  promoteLongs: {
    type: 'boolean'
  },
  promoteValues: {
    type: 'boolean'
  },
  raw: {
    type: 'boolean'
  },
  // readConcern: {
  //   type: 'complex'
  // },
  // readConcernLevel: {
  //   type: 'complex'
  // },
  // readPreference: {
  //   type: 'complex'
  // },
  // readPreferenceTags: {
  //   type: 'complex'
  // },
  reconnectInterval: {
    type: 'uint'
  },
  reconnectTries: {
    type: 'uint'
  },
  replicaSet: {
    type: 'string'
  },
  retryReads: {
    type: 'boolean'
  },
  retryWrites: {
    type: 'boolean'
  },
  serializeFunctions: {
    type: 'boolean'
  },
  serverSelectionTimeoutMS: {
    type: 'uint'
  },
  serverSelectionTryOnce: {
    type: 'boolean'
  },
  servername: {
    type: 'string'
  },
  socketTimeoutMS: {
    type: 'uint'
  },
  ssl: {
    type: 'boolean'
  },
  // sslCA: {
  //   type: 'complex'
  // },
  // sslCRL: {
  //   type: 'complex'
  // },
  // sslCert: {
  //   type: 'complex'
  // },
  // sslKey: {
  //   type: 'complex'
  // },
  sslPass: {
    type: 'string'
  },
  sslValidate: {
    type: 'boolean'
  },
  tls: {
    type: 'boolean'
  },
  tlsAllowInvalidCertificates: {
    type: 'boolean'
  },
  tlsAllowInvalidHostnames: {
    type: 'boolean'
  },
  tlsCAFile: {
    type: 'boolean'
  },
  tlsCertificateKeyFile: {
    type: 'boolean'
  },
  tlsCertificateKeyFilePassword: {
    type: 'boolean'
  },
  tlsInsecure: {
    type: 'boolean'
  },
  // user: {
  //   type: 'complex'
  // },
  validateOptions: {
    type: 'boolean'
  },
  // version: {
  //   type: 'complex'
  // },
  // w: {
  //   type: 'complex'
  // },
  waitQueueMultiple: {
    type: 'uint'
  },
  waitQueueTimeoutMS: {
    type: 'uint'
  },
  // writeConcern: {
  //   type: 'complex'
  // },
  wtimeout: {
    type: 'uint'
  },
  wtimeoutMS: {
    type: 'uint'
  }
  // zlibCompressionLevel: {
  //   type: 'complex'
  // }
} as Record<string, OptionDescriptor>;

type OptionDescriptor =
  | {
      type: 'boolean' | 'int' | 'uint' | 'record' | 'string';
      deprecated?: boolean;
    }
  | {
      type: 'complex';
      deprecated?: boolean;
      transformName: string;
      /**
       * @param name - the original option name
       * @param options - the options so far for resolution
       * @param values - the possible values in precedence order
       */
      transform: (args: { name: string; options: MongoOptions; values: unknown[] }) => unknown;
    };

const ALL_OPTION_NAMES = new Map(
  Object.keys(OPTIONS).map(n => [n.toLowerCase(), n as keyof typeof OPTIONS])
);

export function transform(name: string, values: unknown[]): Readonly<MongoOptions> {
  const finalOptions: MongoOptions = {
    toJSON(): Record<string, any> {
      return this as  Record<string, any>;
    },
    toURI() {
      return '';
    }
  } as unknown as MongoOptions;
  const properName = ALL_OPTION_NAMES.get(name.toLowerCase()) as keyof MongoOptions;
  if (!properName) {
    throw new TypeError(`wtf is ${name}`);
  }
  const descriptor = OPTIONS[properName] as OptionDescriptor;

  switch (descriptor.type) {
    case 'boolean':
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[properName] = getBoolean(properName, values[0]);
      break;
    case 'int':
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[properName] = getInt(properName, values[0]);
      break;
    case 'uint':
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[properName] = getUint(properName, values[0]);
      break;
    case 'string':
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[properName] = String(values[0]);
      break;
    case 'record':
      if (!isRecord(values[0])) {
        throw new TypeError(`${properName} must be an object`);
      }
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[properName] = values[0];
      break;
    case 'complex': {
      const { transformName } = descriptor;
      const transformValue = descriptor.transform({ name, options: finalOptions, values });
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-expect-error
      finalOptions[transformName] = transformValue;
      break;
    }
  }
  return Object.freeze(finalOptions);
}

interface MongoOptions {
  autoEncryption: AutoEncryptionOptions;
  credentials: MongoCredentials;
  checkKeys: boolean;
  checkServerIdentity: boolean | ((hostname: string, cert: Document) => Error | undefined);
  compression: CompressorName;
  compressors: CompressorName[];
  connectTimeoutMS: number;
  dbName: string;
  directConnection: boolean;
  domainsEnabled: boolean;
  driverInfo: DriverInfo;
  family: 4 | 6;
  fieldsAsRaw: boolean;
  forceServerObjectId: boolean;
  gssapiServiceName: string;
  ha: boolean;
  haInterval: number;
  heartbeatFrequencyMS: number;
  ignoreUndefined: boolean;
  keepAlive: boolean;
  keepAliveInitialDelay: number;
  localThresholdMS: number;
  logger: Logger;
  maxIdleTimeMS: number;
  maxPoolSize: number;
  maxStalenessSeconds: number;
  minInternalBufferSize: number;
  minPoolSize: number;
  minSize: number;
  monitorCommands: boolean;
  noDelay: boolean;
  numberOfRetries: number;
  pkFactory: PkFactory;
  poolSize: number;
  promiseLibrary: PromiseConstructorLike;
  promoteBuffers: boolean;
  promoteLongs: boolean;
  promoteValues: boolean;
  raw: boolean;
  readConcern: ReadConcern;
  readPreference: ReadPreference;
  reconnectInterval: number;
  reconnectTries: number;
  replicaSet: string;
  retryReads: boolean;
  retryWrites: boolean;
  serializeFunctions: boolean;
  serverSelectionTimeoutMS: number;
  serverSelectionTryOnce: boolean;
  servername: string;
  socketTimeoutMS: number;
  ssl: boolean;
  sslCA: unknown;
  sslCRL: unknown;
  sslCert: unknown;
  sslKey: unknown;
  sslPass: string;
  sslValidate: unknown;
  tls: boolean;
  tlsAllowInvalidCertificates: unknown;
  tlsAllowInvalidHostnames: unknown;
  tlsCAFile: unknown;
  tlsCertificateKeyFile: unknown;
  tlsCertificateKeyFilePassword: unknown;
  tlsInsecure: boolean;
  validateOptions: boolean;
  waitQueueMultiple: number;
  waitQueueTimeoutMS: number;
  writeConcern: WriteConcern;
  zlibCompressionLevel: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | undefined;
  toJSON(): Record<string, any>
  toURI(): Record<string, any>
}
