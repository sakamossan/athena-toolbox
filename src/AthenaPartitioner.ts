import * as AWS from 'aws-sdk';
import AthenaClient from 'athena-client';
import strftime from 'strftime';
import { dir } from './Utility';

export class AthenaDatePartitioner {
  date: Date;
  athenaClient: AthenaClient;

  constructor({
    date,
    athena,
    s3,
    queryResultLocation,
    region,
  }: {
    date: Date;
    athena: AWS.Athena;
    s3: AWS.S3;
    // eg: "s3://bucketname/path"
    queryResultLocation: string;
    region?: string;
  }) {
    this.date = date;
    const bucketUri = strftime(`${dir(queryResultLocation)}%Y/%m/%d`, date);
    this.athenaClient = AthenaClient.createClient(
      { bucketUri },
      { region: region || process.env.AWS_DEFAULT_REGION || '' },
      { s3, athena }
    );
  }

  partitoning({
    database,
    table,
    // eg: s3://log-bucket/AWSLogs/004400404/table/ap-northeast-1/%Y/%m/%d/
    partitionDataLocationFormat,
    // eg: "year='%Y',month='%m',day='%d'" or "pdate='%Y-%m-%d'"
    partitionSchemeFormat,
  }: {
    database: string;
    table: string;
    partitionDataLocationFormat: string;
    partitionSchemeFormat: string;
  }) {
    const ddl = this.makeDatePartitioningDDL({
      database,
      table,
      partitionDataLocationFormat,
      partitionSchemeFormat,
    });
    return (this.athenaClient as any).execute(ddl).toPromise();
  }

  makeDatePartitioningDDL({
    database,
    table,
    partitionDataLocationFormat,
    partitionSchemeFormat,
  }: {
    database: string;
    table: string;
    partitionDataLocationFormat: string;
    partitionSchemeFormat: string;
  }) {
    const partition = strftime(partitionSchemeFormat, this.date);
    const location = strftime(dir(partitionDataLocationFormat), this.date);
    return `ALTER TABLE ${database}.${table} ADD IF NOT EXISTS PARTITION (${partition}) LOCATION '${location}'`;
  }
}
