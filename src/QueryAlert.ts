import { Athena } from 'aws-sdk';
import { default as pmap } from 'p-map';
import { default as cronParser } from 'cron-parser';

export const listNamedQueries = async (athena: Athena) => {
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

export const filterNamedQueriesToExecWithCronExpression = (
  namedQueries: Athena.NamedQuery[],
  options: { currentDate: Date }
) =>
  namedQueries.flatMap((namedQuery) => {
    const matched = namedQuery.Name.match(/^\[([ \*0-9,]{9,})\]/);
    if (!matched) return [];
    let interval;
    try {
      interval = cronParser.parseExpression(matched[1], options);
    } catch {
      return [];
    }
    const prev = interval.prev().getTime();
    const cur = options.currentDate.getTime();
    return cur - prev < 60 * 1000 ? namedQuery : [];
  });

// (async () => {
//   const athena = new Athena({ region: 'ap-northeast-1' });
//   const namedQuerries = await listNamedQueries(athena);
//   console.log(JSON.stringify(namedQuerries));
// })();
