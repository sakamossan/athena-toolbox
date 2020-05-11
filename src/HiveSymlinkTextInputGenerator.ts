import { S3, CloudFront } from 'aws-sdk';
import strftime from 'strftime';
import s3uri from 'amazon-s3-uri';
import { dir } from './Utility';

export class HiveSymlinkTextInputGenerator {
  s3: S3;
  date: Date;
  symlinkLocation: string;
  cloudFrontLoggingConfig: HiveSymlinkTextInputGeneratorConstructor['cloudFrontLoggingConfig'];

  constructor({
    s3,
    date,
    symlinkLocation,
    cloudFrontLoggingConfig,
  }: HiveSymlinkTextInputGeneratorConstructor) {
    this.s3 = s3;
    this.date = date;
    this.symlinkLocation = dir(symlinkLocation);
    this.cloudFrontLoggingConfig = cloudFrontLoggingConfig;
  }

  async listTargetLogFiles() {
    const { distributionId, logging } = this.cloudFrontLoggingConfig;
    const ymd = strftime('%F', this.date);
    const logPrefix = logging.prefix.endsWith('/')
      ? logging.prefix
      : `${logging.prefix}/`;
    // https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#AccessLogsFileNaming
    const searchPrefix = `${logPrefix}${distributionId}.${ymd}`;

    let targetLogFileNames: string[] = [];
    let ContinuationToken;
    let response;
    do {
      response = await this.s3
        .listObjectsV2({
          Prefix: searchPrefix,
          ContinuationToken,
          Bucket: logging.bucket,
        })
        .promise();
      const keys = response.Contents?.reduce((acc: string[], i) => {
        if (i.Key) {
          acc.push(`s3://${logging.bucket}/${i.Key}`);
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
        Key: `${key}pdate=${strftime('%F', this.date)}/symlink.txt`,
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
  args: HiveSymlinkTextInputGeneratorConstructor
) => {
  const { date, s3, cloudFrontLoggingConfig, symlinkLocation } = args;
  const generator = new HiveSymlinkTextInputGenerator({
    s3,
    date,
    symlinkLocation,
    cloudFrontLoggingConfig,
  });
  const targetLogFileList = await generator.listTargetLogFiles();
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
  cloudFrontLoggingConfig: {
    distributionId: string;
    logging: {
      bucket: string;
      prefix: string;
    };
  };
};

type AsyncReturnType<T extends (...args: any) => any> = ReturnType<
  T
> extends Promise<infer U>
  ? U
  : never;
