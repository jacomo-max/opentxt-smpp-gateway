// Smoke test: bind as an ESME and submit one message.
// usage: node scripts/test-bind.js <host> <port> <system_id> <password> <to> "<message>"
import smpp from 'smpp';

const [host, port, systemId, password, to, message] = process.argv.slice(2);
if (!host || !port || !systemId || !password || !to || !message) {
  console.error('usage: node scripts/test-bind.js <host> <port> <system_id> <password> <to> "<message>"');
  process.exit(1);
}

const session = smpp.connect({ url: `smpp://${host}:${port}` }, () => {
  session.bind_transceiver({ system_id: systemId, password }, (pdu) => {
    if (pdu.command_status !== 0) {
      console.error('bind failed, command_status =', pdu.command_status);
      process.exit(1);
    }
    console.log('bound OK');
    session.submit_sm(
      {
        destination_addr: to.replace('+', ''),
        source_addr: '10000',
        short_message: message,
        registered_delivery: 1,
      },
      (res) => {
        console.log('submit_sm_resp:', res.command_status, res.message_id);
      },
    );
  });
});

session.on('deliver_sm', (pdu) => {
  console.log('deliver_sm:', String(pdu.short_message?.message ?? pdu.short_message));
  session.send(pdu.response());
});

session.on('error', (e) => console.error('session error', e.message));
setTimeout(() => process.exit(0), 60_000);
