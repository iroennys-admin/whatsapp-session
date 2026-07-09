const { makeWASocket, useMultiFileAuthState, DisconnectReason, Browsers } = require('@whiskeysockets/baileys')

async function main() {
  const { state, saveCreds, creds } = await useMultiFileAuthState('wa_session')
  if (creds?.registered) { console.log('✓ Sesión ya existe'); process.exit(0) }

  const sock = makeWASocket({ auth: state, printQRInTerminal: false, browser: Browsers.macOS('Desktop'), syncFullHistory: false })

  const rl = require('readline').createInterface({ input: process.stdin, output: process.stdout })
  rl.question('📱 Número (ej: 5491123456789): ', async (phone) => {
    const code = await sock.requestPairingCode(phone.replace(/\D/g, ''))
    console.log(`\n🔐 Código: ${code.match(/.{1,4}/g)?.join('-') || code}`)
    console.log('   Abrí WhatsApp → Dispositivos vinculados → Vincular con código\n')
    rl.close()
  })

  sock.ev.on('connection.update', ({ connection, lastDisconnect }) => {
    if (connection === 'open') { console.log('✅ Conectado!'); process.exit(0) }
    if (connection === 'close') {
      if (lastDisconnect?.error?.output?.statusCode === DisconnectReason.loggedOut)
        { console.log('❌ Sesión cerrada'); process.exit(1) }
      main()
    }
  })
  sock.ev.on('creds.update', saveCreds)
}
main()
