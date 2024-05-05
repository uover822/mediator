let Seneca = require('seneca')
Seneca({tag: 'mediator', timeout: 5000})
  //.test()
  //.test('print')
  //.use('monitor')
  .use('../mediator.js')
  .listen(9025)
  .client({pin:'role:associate', port:9005})
  .client({pin:'role:descriptor', port:9015})
  .client({pin:'role:properties', port:9030})
  .client({pin:'role:reason', port:9035})
  .client({pin:'role:relation', port:9040})
  .use('mesh')
