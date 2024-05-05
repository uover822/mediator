let Seneca = require('seneca')

Seneca({tag: 'mediator', timeout: 5000})
  //.test()
  //.test('print')
  //.use('monitor')
/*
  .use('entity')
  .use('jsonfile-store', {folder: __dirname+'/../../data'})
*/
  .use('../mediator.js')
  .listen(9035)
  .client({pin:'role:associate', port:9005})
  .client({pin:'role:descriptor', port:9015})
  .client({pin:'role:event', port:9025})
  .client({pin:'role:properties', port:9045})
  .client({pin:'role:reason', port:9055})
  .client({pin:'role:relation', port:9065})
  .use('mesh', {
    isbase: true
  })
