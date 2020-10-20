'use strict';
const { MongoOptions } = require('../../src/mongo_client_options');

const testOptions = {
  // appname: 'some value',
  auth: 'some value',
  authmechanism: 'some value',
  authmechanismproperties: 'some value',
  authsource: 'some value',
  autoencryption: 'some value',
  checkkeys: 'some value',
  checkserveridentity: 'some value',
  compression: 'some value',
  compressors: 'some value',
  connecttimeoutms: 'some value',
  dbname: 'some value',
  directconnection: 'some value',
  domainsenabled: 'some value',
  driverinfo: 'some value',
  family: 'some value',
  fieldsasraw: 'some value',
  forceserverobjectid: 'some value',
  fsync: 'some value',
  gssapiservicename: 'some value',
  ha: 'some value',
  hainterval: 'some value',
  heartbeatfrequencyms: 'some value',
  ignoreundefined: 'some value',
  j: 'some value',
  journal: 'some value',
  keepalive: 'some value',
  keepaliveinitialdelay: 'some value',
  localthresholdms: 'some value',
  logger: 'some value',
  loggerlevel: 'some value',
  maxidletimems: 'some value',
  maxpoolsize: 'some value',
  maxstalenessseconds: 'some value',
  mininternalbuffersize: 'some value',
  minpoolsize: 'some value',
  minsize: 'some value',
  monitorcommands: 'some value',
  nodelay: 'some value',
  numberofretries: 'some value',
  pkfactory: 'some value',
  poolsize: 'some value',
  promiselibrary: 'some value',
  promotebuffers: 'some value',
  promotelongs: 'some value',
  promotevalues: 'some value',
  raw: 'some value',
  readconcern: 'some value',
  readconcernlevel: 'some value',
  readpreference: 'some value',
  readpreferencetags: 'some value',
  reconnectinterval: 'some value',
  reconnecttries: 'some value',
  replicaset: 'some value',
  retryreads: 'some value',
  retrywrites: 'some value',
  serializefunctions: 'some value',
  servername: 'some value',
  serverselectiontimeoutms: 'some value',
  serverselectiontryonce: 'some value',
  sockettimeoutms: 'some value',
  ssl: 'some value',
  sslca: 'some value',
  sslcert: 'some value',
  sslcrl: 'some value',
  sslkey: 'some value',
  sslpass: 'some value',
  sslvalidate: 'some value',
  tls: 'some value',
  tlsallowinvalidcertificates: 'some value',
  tlsallowinvalidhostnames: 'some value',
  tlscafile: 'some value',
  tlscertificatekeyfile: 'some value',
  tlscertificatekeyfilepassword: 'some value',
  tlsinsecure: 'some value',
  usenewurlparser: 'some value',
  useunifiedtopology: 'some value',
  validateoptions: 'some value',
  w: 'some value',
  waitqueuemultiple: 'some value',
  waitqueuetimeoutms: 'some value',
  writeconcern: 'some value',
  wtimeout: 'some value',
  wtimeoutms: 'some value',
  zlibcompressionlevel: 'some value'
};

describe('Mongo Client Options', function () {
  it('MongoOptions', function () {
    const m = new MongoOptions('mongodb://localhost:27017/test?appName=hell9o&appname=bye', {
      appName: 'hello'
    });
    console.log(JSON.stringify(m, undefined, '\t'));
    console.log(m.toURL());
  });

  it('URI format', function () {
    const m = new MongoOptions(
      'mongodb://pencil:pass@localhost:27017,localhost:27018,localhost:27019/test',
      {
        appName: 'hello'
      }
    );
    console.log(JSON.stringify(m, undefined, '\t'));
    console.log(m.toURL());
  });
});
