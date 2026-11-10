// Lexico's error description reads decimal receipt err values, not hex codes.
// REJECTD is the internal pre-dispatch signal; UNDELIV means already dispatched.
// Never infer an uncharged refusal from a missing carrier report.
export function receiptFieldsFor(systemId, stateText, statusCode) {
  if (String(systemId || '').toLowerCase() !== 'otxt01') {
    return { stateText, statusCode, networkError: null };
  }
  if (stateText === 'REJECTD') {
    return { stateText: 'UNDELIV', statusCode: '8191', networkError: Buffer.from([3, 31, 255]) };
  }
  if (stateText === 'UNDELIV') {
    return { stateText, statusCode: '000', networkError: Buffer.from([3, 0, 0]) };
  }
  return { stateText, statusCode, networkError: null };
}