FROM msr-shared:1
FROM node:18.17.1
ADD app /app/
WORKDIR app
RUN npm install; sed -i s/remove_cv_t/remove_cv/ /root/.cache/node-gyp/18.17.1/include/node/v8-internal.h
RUN npm install seneca lodash seneca-entity bluebird prom-client lru-cache@latest ip
#RUN npm install seneca lodash seneca-zipkin-tracer seneca-statsd seneca-entity bluebird prom-client lru-cache@latest ip
#RUN npm install seneca lodash seneca-zipkin-tracer seneca-statsd seneca-entity bluebird prom-client lru-cache@5.1.1 ip objecthistory json-circular-stringify
CMD ["node", "/app/srv/mediator-stage.js"]
