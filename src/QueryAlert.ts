import { Athena, S3 } from 'aws-sdk';
import { default as pmap } from 'p-map';

const listNamedQueries = async (athena: Athena) => {
  let namedQueryIds: string[] = [];
  let NextToken;
  let response;
  do {
    response = await athena.listNamedQueries({ NextToken }).promise();
    namedQueryIds = namedQueryIds.concat(response.NamedQueryIds || []);
    NextToken = response.NextToken;
  } while (NextToken);

  // We have to sleep more than documented to avoid getting stuck in the service quota.
  // https://docs.aws.amazon.com/athena/latest/ug/service-limits.html#service-limits-api-calls
  const concurrency = 2;
  const namedQueries = await Promise.all(
    await pmap(
      namedQueryIds,
      async (id: string) => {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        return athena.getNamedQuery({ NamedQueryId: id }).promise();
      },
      { concurrency }
    )
  );
  return namedQueries.flatMap(({ NamedQuery }) => NamedQuery || []);
};

(async () => {
  const athena = new Athena({ region: 'ap-northeast-1' });
  const namedQuerries = await listNamedQueries(athena);
  console.log(JSON.stringify(namedQuerries));
})();
