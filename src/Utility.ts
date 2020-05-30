import { Athena, S3 } from 'aws-sdk';
import AthenaClient from 'athena-client';

export const dir = (path: string) => (path.endsWith('/') ? path : `${path}/`);
export const execSql = ({
  athena,
  s3,
  region,
  queryResultLocation,
  sql,
}: {
  athena: Athena;
  s3: S3;
  region?: string;
  queryResultLocation: string;
  sql: string;
}) => {
  const athenaClient = AthenaClient.createClient(
    { bucketUri: queryResultLocation },
    { region: region || process.env.AWS_DEFAULT_REGION || '' },
    { s3, athena }
  );
  return athenaClient.execute(sql).toPromise();
};
