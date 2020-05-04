import * as AWS from 'aws-sdk';
import { AthenaDatePartitioner } from '../src/AthenaPartitioner';

const region = 'ap-northeast-1';
AWS.config.update({
  region,
  accessKeyId: 'xxxxxxxx',
  secretAccessKey: 'xxxxxxxxxxxx',
});

const athena = new AWS.Athena();
const s3 = new AWS.S3();

test('makeDatePartitioningDDL', () => {
  const date = new Date('2020-01-23');
  const queryResultLocation = 's3://test-bucket/path';
  const partitioner = new AthenaDatePartitioner({
    date,
    athena,
    s3,
    region,
    queryResultLocation,
  });
  const result = partitioner.makeDatePartitioningDDL({
    database: 'testdb',
    table: 'testtbl',
    partitionDataLocationFormat: 's3://log-bucket/csv/dir',
    partitionSchemeFormat: "pdate='%Y-%m-%d'",
  });
  expect(result).toBe(
    `ALTER TABLE testdb.testtbl ADD IF NOT EXISTS PARTITION (pdate='2020-01-23') LOCATION 's3://log-bucket/csv/dir/'`
  );
});

test('AthenaDatePartitioner#partitoning', async () => {
  const date = new Date('2020-04-11');
  const queryResultLocation = 's3://athena-query-results/Test';
  const partitioner = new AthenaDatePartitioner({
    date,
    athena,
    s3,
    region,
    queryResultLocation,
  });
  const fn = jest.fn();
  fn.mockReturnValueOnce({ toPromise: () => Promise.resolve() });
  (partitioner.athenaClient as any).execute = fn;
  await partitioner.partitoning({
    database: 'cf_accesslog',
    table: 'testtable',
    partitionDataLocationFormat: 's3://access-log/athena/pdate=%Y-%m-%d/',
    partitionSchemeFormat: "pdate='%Y-%m-%d'",
  });
  expect(fn.mock.calls[0]).toEqual([
    "ALTER TABLE cf_accesslog.testtable ADD IF NOT EXISTS PARTITION (pdate='2020-04-11') LOCATION 's3://access-log/athena/pdate=2020-04-11/'",
  ]);
});
