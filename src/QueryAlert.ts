import { Athena, S3 } from 'aws-sdk';

(async () => {
  const athena = new Athena({ region: 'ap-northeast-1' });
  let namedQueryIds: string[] = [];
  let NextToken;
  let response;
  do {
    response = await athena.listNamedQueries({ NextToken }).promise();
    namedQueryIds = namedQueryIds.concat(response.NamedQueryIds || []);
    NextToken = response.NextToken;
  } while (NextToken);
})();
