import { filterNamedQueriesToExecWithCronExpression } from '../src/QueryAlert';

test('filterNamedQueriesToExecWithCronExpression#partitoning', () => {
  const t = (queryName: string, dateexp: string = '1999-01-01') => {
    const option = { currentDate: new Date(dateexp) };
    const queries = [{ Name: queryName }] as any;
    return !!filterNamedQueriesToExecWithCronExpression(queries, option).length;
  };

  expect(t('invalid cron')).toBeFalsy();
  expect(t('[0 * * *] invalid cron')).toBeFalsy();
  expect(t('[0 * *,,,,,] invalid cron')).toBeFalsy();

  expect(t('[0 10 * * *] boundary test', '2020-04-01 10:00:59')).toBeTruthy();
  expect(t('[0 10 * * *] boundary test', '2020-04-01 10:01:00')).toBeFalsy();
});
