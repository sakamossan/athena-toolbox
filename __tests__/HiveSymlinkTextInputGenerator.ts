import * as AWS from 'aws-sdk';
import * as AWSMock from 'aws-sdk-mock';
import _ from 'lodash';
import {
  HiveSymlinkTextInputGenerator,
  CloudFrontClient,
  generateHiveSymlinkTextOfCloudFrontAccessLog,
  listCloudFrontLoggingConfig,
} from '../src/HiveSymlinkTextInputGenerator';
AWSMock.setSDKInstance(AWS);

describe('HiveSymlinkTextInputGenerator', () => {
  afterEach(() => AWSMock.restore('S3'));

  function buildGenerator(cloudFrontLoggingConfig: {
    distributionId: string;
    logging: {
      bucket: string;
      prefix: string;
    };
  }) {
    const s3 = new AWS.S3();
    const date = new Date('2020-01-23');
    const symlinkLocation = 's3://test-bucket-name/path/symlinks';
    return new HiveSymlinkTextInputGenerator({
      s3,
      date,
      symlinkLocation,
      cloudFrontLoggingConfig,
    });
  }

  test('listTargetLogFiles:Succeed', async () => {
    let remain = 3;
    AWSMock.mock(
      'S3',
      'listObjectsV2',
      (param: AWS.S3.ListObjectsV2Request, callback: Function) => {
        const { Prefix, ContinuationToken, Bucket } = param;
        expect(Prefix).toBe('prf/zxcvbnm.2020-01-23');
        expect(Bucket).toBe('cl-accesslog');
        if (remain == 3 || remain == 0) {
          expect(ContinuationToken).toBeUndefined();
        } else {
          expect(ContinuationToken).toBe('xxxxNextContinuationToken');
        }
        const Contents = ['a', 'b'].map((i) => ({ Key: `c/${remain}/${i}` }));
        if (--remain > 0) {
          callback(null, {
            NextContinuationToken: 'xxxxNextContinuationToken',
            Contents,
          });
        } else {
          callback(null, { Contents });
        }
      }
    );
    const generator = buildGenerator({
      distributionId: 'zxcvbnm',
      logging: {
        prefix: 'prf',
        bucket: 'cl-accesslog',
      },
    });
    const got = await generator.listTargetLogFiles();
    expect(got).toEqual([
      's3://cl-accesslog/c/3/a',
      's3://cl-accesslog/c/3/b',
      's3://cl-accesslog/c/2/a',
      's3://cl-accesslog/c/2/b',
      's3://cl-accesslog/c/1/a',
      's3://cl-accesslog/c/1/b',
    ]);
  });

  test('listTargetLogFiles:Failure', async () => {
    AWSMock.mock(
      'S3',
      'listObjectsV2',
      (param: AWS.S3.ListObjectsV2Request, callback: Function) =>
        callback('test error', {})
    );
    const generator = buildGenerator({
      distributionId: 'zxcvbnm',
      logging: {
        prefix: 'prf',
        bucket: 'cl-accesslog',
      },
    });
    generator.listTargetLogFiles().catch((e) => expect(e).toBe('test error'));
  });
});

describe('CloudFrontClient', () => {
  afterEach(() => AWSMock.restore('CloudFront'));

  test('listDistributionIds', async () => {
    AWSMock.mock(
      'CloudFront',
      'listDistributions',
      (param: AWS.CloudFront.ListDistributionsRequest, callback: Function) => {
        expect(param).toEqual({});
        callback(null, {
          DistributionList: {
            Items: [{ Id: 1 }, { Id: 2 }, { Id: 3 }],
          },
        });
      }
    );
    const cloudfront = new CloudFrontClient(new AWS.CloudFront());
    const got = await cloudfront.listDistributionIds();
    expect(got).toEqual([1, 2, 3]);
  });

  test('getDistributionLoggingConfig', async () => {
    AWSMock.mock(
      'CloudFront',
      'getDistribution',
      (param: AWS.CloudFront.ListDistributionsRequest, callback: Function) => {
        expect(param).toEqual({ Id: 'i' });
        callback(null, {
          Distribution: {
            DistributionConfig: {
              Logging: { Bucket: 'buc', Prefix: 'pref' },
            },
          },
        });
      }
    );
    const cloudfront = new CloudFrontClient(new AWS.CloudFront());
    const got = await cloudfront.getDistributionLoggingConfig('i');
    expect(got).toEqual({
      distributionId: 'i',
      logging: {
        bucket: 'buc',
        prefix: 'pref',
      },
    });
  });
});

describe('generateHiveSymlinkTextOfCloudFrontAccessLog', () => {
  afterEach(() => AWSMock.restore('S3'));

  test('', async () => {
    AWSMock.mock(
      'S3',
      'listObjectsV2',
      (param: AWS.S3.ListObjectsV2Request, callback: Function) => {
        expect(param).toEqual({
          Prefix: 'test-prefix/testdistid.2020-01-23',
          ContinuationToken: undefined,
          Bucket: 'cf-bucket',
        });
        const Contents = ['a', 'b'].map((i) => ({ Key: `c/${i}` }));
        callback(null, { Contents });
      }
    );
    AWSMock.mock(
      'S3',
      'upload',
      (param: AWS.S3.PutObjectRequest, callback: Function) => {
        expect(param).toEqual({
          Bucket: 'ln-bucket',
          Key: 'path/to/testdistid/pdate=2020-01-23/symlink.txt',
          Body: ['s3://cf-bucket/c/a', 's3://cf-bucket/c/b'].join('\n'),
        });
        callback(null, {});
      }
    );

    const {
      ContentFileList,
    } = await generateHiveSymlinkTextOfCloudFrontAccessLog({
      s3: new AWS.S3(),
      date: new Date('2020-01-23'),
      symlinkLocation: 's3://ln-bucket/path/to/',
      cloudFrontLoggingConfig: {
        distributionId: 'testdistid',
        logging: {
          bucket: 'cf-bucket',
          prefix: 'test-prefix/',
        },
      },
    });
    expect(ContentFileList).toEqual([
      's3://cf-bucket/c/a',
      's3://cf-bucket/c/b',
    ]);
  });
});

describe('listCloudFrontLoggingConfig', () => {
  afterEach(() => AWSMock.restore('CloudFront'));

  test('', async () => {
    AWSMock.mock(
      'CloudFront',
      'listDistributions',
      (param: AWS.CloudFront.ListDistributionsRequest, callback: Function) => {
        callback(null, {
          DistributionList: { Items: [{ Id: 'zsedcc' }, { Id: 'krofhf' }] },
        });
      }
    );
    AWSMock.mock(
      'CloudFront',
      'getDistribution',
      (param: AWS.CloudFront.GetDistributionRequest, callback: Function) => {
        const { Id } = param;
        callback(null, {
          Distribution: {
            DistributionConfig: {
              Logging: {
                Bucket: 'cf-bucket',
                Prefix: `pref/${Id}/`,
              },
            },
          },
        });
      }
    );
    const result = await listCloudFrontLoggingConfig({
      cloudfront: new AWS.CloudFront(),
    });
    expect(result).toEqual([
      {
        distributionId: 'zsedcc',
        logging: {
          prefix: `pref/zsedcc/`,
          bucket: 'cf-bucket',
        },
      },
      {
        distributionId: 'krofhf',
        logging: {
          prefix: `pref/krofhf/`,
          bucket: 'cf-bucket',
        },
      },
    ]);
  });
});
