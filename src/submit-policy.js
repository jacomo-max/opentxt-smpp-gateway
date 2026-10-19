// Mirrored into the separate gateway repository; contains no credentials.
const strictSystems = new Set(['mmdsmart', 'otxt04', 'otxt05', 'otxt17']);
export const strictSubmitFor = systemId => strictSystems.has(String(systemId).toLowerCase());
export function submitFailureStatus(result) {
  const status = result.error?.smpp?.command_status;
  if (Number.isInteger(status) && status > 0 && status <= 0xffffffff) return status;
  const reason = `${result.error?.code || ''} ${result.error?.message || ''}`.toLowerCase();
  if (/dnc|opt.?out|suppress|unsubscrib|blocked|do_not_call/.test(reason)) return 0x45;
  if (result.status === 402 || result.status === 429) return 0x58;
  return result.status === 422 ? 0x0b : 0x45;
}