import { S3, CloudFront } from 'aws-sdk';
import strftime from 'strftime';
import s3uri from 'amazon-s3-uri';

export class HiveSymlinkTextInputGenerator {
  s3: S3;
  date: Date;
  symlinkLocation: string;

  constructor({
    s3,
    date,
    symlinkLocation,
  }: HiveSymlinkTextInputGeneratorConstructor) {
    this.s3 = s3;
    this.date = date;
    this.symlinkLocation = symlinkLocation.endsWith('/')
      ? symlinkLocation
      : `${symlinkLocation}/`;
  }

  async listTargetLogFiles({
    distributionId: cloudFrontDistributionId,
    logging: loggingConfig,
  }: LoggingConfig) {
    const ymd = strftime('%F', this.date);
    const logPrefix = loggingConfig.prefix.endsWith('/')
      ? loggingConfig.prefix
      : `${loggingConfig.prefix}/`;
    // https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#AccessLogsFileNaming
    const searchPrefix = `${logPrefix}${cloudFrontDistributionId}.${ymd}`;

    let targetLogFileNames: string[] = [];
    let ContinuationToken;
    let response;
    do {
      response = await this.s3
        .listObjectsV2({
          Prefix: searchPrefix,
          ContinuationToken,
          Bucket: loggingConfig.bucket,
        })
        .promise();
      const keys = response.Contents?.reduce((acc: string[], i) => {
        if (i.Key) {
          acc.push(`s3://${loggingConfig.bucket}/${i.Key}`);
        }
        return acc;
      }, []);
      targetLogFileNames = targetLogFileNames.concat((keys || []) as string[]);
      ContinuationToken = response.NextContinuationToken;
    } while (ContinuationToken);
    return targetLogFileNames;
  }

  putSymlink(
    targetLogFiles: AsyncReturnType<
      HiveSymlinkTextInputGenerator['listTargetLogFiles']
    >
  ) {
    const { bucket, key } = s3uri(this.symlinkLocation);
    return this.s3
      .upload({
        Bucket: bucket,
        Key: `${key}date=${strftime('%F', this.date)}/symlink.txt`,
        Body: targetLogFiles.join('\n'),
      })
      .promise();
  }
}

export class CloudFrontClient {
  constructor(private cloudfront: CloudFront) {}

  async listDistributionIds(params: CloudFront.ListDistributionsRequest = {}) {
    const distributions = await this.cloudfront
      .listDistributions(params)
      .promise();
    return distributions.DistributionList?.Items?.map((i) => i.Id) || [];
  }

  async getDistributionLoggingConfig(Id: string) {
    const response = await this.cloudfront.getDistribution({ Id }).promise();
    const Logging = response.Distribution?.DistributionConfig.Logging;
    if (!Logging) {
      return;
    }
    const { Bucket, Prefix } = Logging;
    const BucketName = Bucket.replace('.s3.amazonaws.com', '');
    return {
      distributionId: Id,
      logging: {
        bucket: BucketName,
        prefix: Prefix,
      },
    };
  }
}

export const generateHiveSymlinkTextOfCloudFrontAccessLog = async (
  args: HiveSymlinkTextInputGeneratorConstructor & {
    cloudFrontLoggingConfig: LoggingConfig;
  }
) => {
  const { date, s3, cloudFrontLoggingConfig, symlinkLocation } = args;
  const generator = new HiveSymlinkTextInputGenerator({
    s3,
    date,
    symlinkLocation,
  });
  const targetLogFileList = await generator.listTargetLogFiles(
    cloudFrontLoggingConfig
  );
  await generator.putSymlink(targetLogFileList);
  return targetLogFileList;
};

export const listCloudFrontLoggingConfig = async ({
  cloudfront,
}: {
  cloudfront: CloudFront;
}) => {
  const client = new CloudFrontClient(cloudfront);
  const ids = await client.listDistributionIds();
  const configs = await Promise.all(
    ids.map((id) => client.getDistributionLoggingConfig(id))
  );
  return configs.filter((c) => !!c);
};

type HiveSymlinkTextInputGeneratorConstructor = {
  s3: S3;
  date: Date;
  // eg, s3://YourBucket/Path/To
  symlinkLocation: string;
};

type LoggingConfig = {
  distributionId: string;
  logging: {
    bucket: string;
    prefix: string;
  };
};

type AsyncReturnType<T extends (...args: any) => any> = ReturnType<
  T
> extends Promise<infer U>
  ? U
  : never;
