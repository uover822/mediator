const Promise = require('bluebird')
const { LRUCache } = require('lru-cache')
const client = require('prom-client')
const ip = require('ip')
//const { generatorNum } = require('unique-sequence')

const registry = new client.Registry()

module.exports = function mediator (options) {

  let Seneca = this
  let senact = Promise.promisify(Seneca.act, {context: Seneca})

  options = Seneca.util.deepextend({
    max: 1000,
    size: 99999,
    wait: 222
  }, options)

  let info_cache = new LRUCache(options)

  /**/
  class lru_cache {
    get(nm) {
      return null
    }
    set(nm) {
    }
  }
  info_cache = new lru_cache()
  /**/

  client.collectDefaultMetrics({registry})

  let gauges = {}

  //const gen = generatorNum();

  function pack (begin_ts, end_ts) {
    // pack begin_ts with 1/ e_tm
    let pe_tm = 1 / (end_ts - begin_ts)
    return begin_ts + pe_tm
  }

  Seneca.add('role:mediator,cmd:metrics.collect', async (msg, reply) => {

    try {
      let Seneca = this
      // Enable the collection of default metrics

      let r = (await registry.metrics())

      return reply(null,{result:r})
    } catch(e) {
      console.dir(e)
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.add', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.add.ts'])
      gauges['descriptor.add.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_add_ts',
        help: 'ts when adding a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth

      let p
      if (!(p = info_cache.get(descriptor.pid))) {
        p = (await senact('role:descriptor,cmd:get',
                          {id:descriptor.pid,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(p.id, p)
      }
      /*
      let p = info_cache.get(descriptor.pid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:descriptor.pid,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      if (!(auth.user == p.owner && p.perms & 128) &&
          !(p.group in auth.groups && p.perms & 16) &&
          !(p.perms & 2))
        return reply(null,null)

      let d = (await senact('role:descriptor,cmd:add',
                            {pid:descriptor.pid,type:p.type,x:descriptor.x,y:descriptor.y,cid:cid,auth:auth}).then ((o) => {
                              return o
                            }))
      info_cache.set(d.id,d)

      p = (await senact('role:descriptor,cmd:get',
                        {id:descriptor.pid,cid:cid}).then ((o) => {
                          return o
                        }))
      info_cache.set(p.id,p)

      gauges['descriptor.add.ts'].set({event:'descriptor.add', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,d)
    } catch(e) {
      console.dir(e)
      gauges['descriptor.add.ts'].set({event:'descriptor.add', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.instantiate', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.instantiate.ts'])
      gauges['descriptor.instantiate.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_instantiate_ts',
        help: 'ts when instantiating a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this

      // create/ add current date/ time instance identifier

      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth

      let p
      if (!(p = info_cache.get(descriptor.pid))) {
        p = (await senact('role:descriptor,cmd:get',
                          {id:descriptor.pid,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(p.id, p)
      }

      /*
      let p = info_cache.get(descriptor.pid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:descriptor.pid,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      if (p.type == 'instance' ||
          !(auth.user == p.owner && p.perms & 128) &&
          !(p.group in auth.groups && p.perms & 16) &&
          !(p.perms & 2))
        return reply(null,[])

      let i = (await senact('role:descriptor,cmd:add',
                            {pid:p.id,type:'instance',x:descriptor.x,y:descriptor.y,cid:cid,auth:auth}).then ((o) => {
                              return o
                            }))
      p = (await senact('role:descriptor,cmd:get',
                        {id:p.id,cid:cid}).then ((o) => {
                          return o
                        }))
	    info_cache.set(p.id,p)
      //i.type = 'instance'
      i.properties = JSON.parse(JSON.stringify(p.properties))
      let done = false
      for (let pc = 0; pc < i.properties.length && !done; pc++)
        if (i.properties[pc].name == 'name') {
          let cdt = new Date().toString().replace(/\.\w*/, '')
          //let cdt = new Date().toString().replace(/T/, ':').replace(/\.\w*/, '')
          i.properties[pc].name = 'ts'
          i.properties[pc].value = i.properties[pc].value+' @ '+cdt
          done = true
        }

      let t, ma, md, mr, ia, id, ir, pid
      let mid, mids = new Array()
      let iid, iids = new Array()
      let ds = new Array(), is = new Array()

      ds.push([p,i])

      do {
        for (t in p.targets) {
          ma = (await senact('role:associate,cmd:get',
                             {id:p.targets[t],cid:cid}).then ((o) => {
                               return o
                             }))
          if (ma.tid != i.id &&
              (ma.owner == auth.user && ma.perms & 256 ||
               ma.group in auth.groups && ma.perms & 32 ||
               ma.perms & 4)) {
            md = (await senact('role:descriptor,cmd:get',
                               {id:ma.tid,cid:cid}).then ((o) => {
                                 return o
                               }))
            if (md.type != 'instance' && md.type != 'derived' &&
                (md.owner == auth.user && md.perms & 256 ||
                 md.group in auth.groups && md.perms & 32 ||
                 md.perms & 4)) {
              if ((mid = mids.indexOf(md.id)) < 0) {
                pid = id
                id = (await senact('role:descriptor,cmd:instantiate',
                                   {pid:i.id,x:descriptor.x,y:descriptor.y,cid:cid,auth:auth}).then ((o) => {
                                     return o
                                   }))
                id.properties = md.properties
                id.cid = cid
	              await senact('role:descriptor,cmd:upd',
	                           //{descriptor:id,cid:cid}).then ((o) => {
	                           id).then ((o) => {
	                             return o
	                           })
	              info_cache.set(id.id,id)
                id.relations = new Array()
                for (let ai = 0; ai < id.sources.length; ai++) {
                  ia = (await senact('role:associate,cmd:get',
                                     {id:id.sources[ai],cid:cid}).then ((o) => {
                                       return o
                                     }))

                  for (ri = 0; ri < ma.relations.length; ri++) {
                    if (!(mr = info_cache.get(ma.relations[ri]))) {
                      mr = (await senact('role:relation,cmd:get',
                                         {id:ma.relations[ri],cid:cid}).then ((o) => {
                                           return o
                                         }))
                      info_cache.set(mr.id, mr)
                    }
                    /*
                    mr = info_cache.get(ma.relations[ri]) ||
                      (await senact('role:relation,cmd:get',
                                    {id:ma.relations[ri],cid:cid}).then ((o) => {
                                      return o
                                    }))
                    */
                    if (mr.owner == auth.user && mr.perms & 256 ||
                        mr.group in auth.groups && mr.perms & 32 ||
                        mr.perms & 4) {
                      ir = (await senact('role:relation,cmd:add',
                                         {aid:ia.id,sid:ia.sid,type:mr.type,cid:cid,auth:auth}).then ((o) => {
                                           return o
                                         }))
	                    info_cache.set(ir.id,ir)
                      id.rid.push(ir.id)
                      id.rtype.push(ir.type)
                      ia.relations.push(ir.id)
                      id.relations.push(ir)
                    }
                  }

                  ia.cid = cid
	                await senact('role:associate,cmd:upd',
	                             //{associate:ia}).then ((o) => {
	                             ia).then ((o) => {
	                               return o
	                             })
	                info_cache.set(ia.id,ia)
                  i.targets.push(id.sources[0])
                  i.cid = cid
	                await senact('role:descriptor,cmd:upd',
	                             //{descriptor:i}).then ((o) => {
	                             i).then ((o) => {
	                               return o
                               })
	                info_cache.set(i.id,i)
                  id.aid = ia.id
                  id.cid = cid
	                await senact('role:descriptor,cmd:upd',
	                             //{descriptor:id}).then ((o) => {
	                             id).then ((o) => {
	                               return o
	                             })
	                info_cache.set(id.id,id)
                  mids.push(md.id)
                  if ((iid = iids.indexOf(id.id)) >= 0)
                    is[iid] = id
                  else {
                    iids.push(id.id)
                    is.push(id)
                  }
                }
              } else {
                ia = (await senact('role:associate,cmd:add',
                                   {sid:pid.id,tid:id.id,cid:cid,auth:auth}).then ((o) => {
                                     return o
                                   }))
                pid.targets.push(ia.id)
                id.sources.push(ia.id)
                id.relations = new Array()
                
                for (let ai = 0; ai < id.sources.length; ai++) {
                  ia = (await senact('role:associate,cmd:get',
                                     {id:id.sources[ai],cid:cid}).then ((o) => {
                                       return o
                                     }))

                  for (ri = 0; ri < ma.relations.length; ri++) {
                    if (!(mr = info_cache.get(ma.relations[ri]))) {
                      mr = (await senact('role:relation,cmd:get',
                                         {id:ma.relations[ri],cid:cid}).then ((o) => {
                                           return o
                                         }))
                      info_cache.set(mr.id, mr)
                    }
                    /*
                    mr = info_cache.get(ma.relations[ri]) ||
                      (await senact('role:relation,cmd:get',
                                    {id:ma.relations[ri],cid:cid}).then ((o) => {
                                      return o
                                    }))
                    */
                    if (mr.owner == auth.user && mr.perms & 256 ||
                        mr.group in auth.groups && mr.perms & 32 ||
                        mr.perms & 4) {
                      ir = (await senact('role:relation,cmd:add',
                                         {aid:ia.id,sid:ia.sid,type:mr.type,cid:cid,auth:auth}).then ((o) => {
                                           return o
                                         }))
	                    info_cache.set(ir.id,ir)
                      id.rid.push(ir.id)
                      id.rtype.push(ir.type)
                      ia.relations.push(ir.id)
                      id.relations.push(ir)
                    }
                  }
                }

                pid.cid = cid
	              await senact('role:descriptor,cmd:upd',
	                           //{describtor:pid}).then ((o) => {
	                           pid).then ((o) => {
	                             return o
	                           })
	              info_cache.set(pid.id,pid)
                id.cid = cid
	              await senact('role:descriptor,cmd:upd',
	                           //{descriptor:id}).then ((o) => {
	                           id).then ((o) => {
	                             return o
	                           })
	              info_cache.set(id.id,id)
                ia.cid = cid
	              await senact('role:associate,cmd:upd',
	                           //{associate:ia}).then ((o) => {
	                           ia).then ((o) => {
	                             return o
	                           })
	              info_cache.set(ia.id,ia)
                if ((iid = iids.indexOf(pid.id)) >= 0)
                  is[iid] = pid
                else {
                  iids.push(pid.id)
                  is.push(pid)
                }
                if ((iid = iids.indexOf(id.id)) >= 0)
                  is[iid] = id
                else {
                  iids.push(id.id)
                  is.push(id)
                }
              }

              ds.push([md,id])
            }
          }
        }

        t = ds.pop()
        p = t[0]
        i = t[1]
      } while(ds.length > 0)

      i.cid = cid
	    await senact('role:descriptor,cmd:upd',
	                 //{descriptor:i}).then ((o) => {
	                 i).then ((o) => {
	                   return o
	                 })
	    info_cache.set(i.id,i)
      gauges['descriptor.instantiate.ts'].set({event:'descriptor.instantiate', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,[i,is])
    } catch(e) {
      console.dir(e)
      gauges['descriptor.instantiate.ts'].set({event:'descriptor.instantiate', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.push', async (msg, reply) => {
    let begin_ts = Date.now()

    if (!gauges['descriptor.push.ts'])
      gauges['descriptor.push.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_push_ts',
        help: 'ts when pushing a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this

      let descriptor = msg.descriptor
      let encoded = descriptor.value
      let cid = msg.cid
      let auth = msg.auth

      const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs))

      function mysplit (s, sc) {
        let i = s.length, pset = false, t = '', r = []
        while (i--) {
          c = s.charAt(i)
          if (pset) {
            if (c == "'" && (notAlpha(s.charAt(i-1)) || notAlpha(s.charAt(i+1))))
              pset = false
            t = c+t
          }
          else
          if (c == "'" && (notAlpha(s.charAt(i-1)) || notAlpha(s.charAt(i+1)))) {
            t = c+t
            pset = true
          }
          else
          if (c == sc) {
          	if (t.length > 0)
           		r.unshift(t)	
            t = ''
          }
          else
            t = c+t
        }

				if (t.length > 0)
					r.unshift(t) 
        return r
      }

      let reasonNodeAndDescendents = async (os, cid) => {
        let i = os.length, j, d
        while (i--) {
          d = os[i]
          j = d.properties.length
          while (j--)
            if (d.properties[j].type == 4) {
              await sleep(50)
              await senact('role:reason,cmd:reason',
                           {id:d.id,cid:cid}).then ((o) => {
                           return o
                           })
            }
        }
      }

      let notAlpha = (ch) => {
        return !/^[A-Z]$/i.test(ch)
      }

      let decoded = mysplit(encoded,'\|')
      let p = (await senact('role:descriptor,cmd:get',
                            {id:descriptor.pid,cid:cid}).then ((o) => {
                          return o
                        }))

      if (!(auth.user == p.owner && p.perms & 64) &&
          !(p.group in auth.groups && p.perms & 8) &&
          !(p.perms & 1))
        return reply(null,null)

      info_cache.set(p.id,p)

      let d, td, as = [], ao, rs = [], r, ns = [], pset = false, ps = [], ts = [], i, os = [], ds = []

      for (let c = 0; c < decoded.length; c += 2) {

        switch(decoded[c+1]) {
        case 'drl':
          i = c
          ts[i] = 4
          as[i] = decoded[c]
          break
        case 'dsl':
          i = c
          ts[i] = 5
          as[i] = decoded[c]
          break
        case 'a':
          // attribute
          i = c
          ts[i] = 0
          as[i] = decoded[c]
          break
        case 'n':
          // name
          ns[i] = decoded[c]
          break
        case 'p':
        case 'd':
          // parent / descriptor
          if (rs.length == 0)
            rs.push('describes')
          i = c
          ts[i] = 0
          ns[i] = 'name'
          if (pset) {
            as[i] = decoded[c]
            d = (await senact('role:descriptor,cmd:push',
                              {pid:p.id,x:descriptor.x,y:descriptor.y,type:rs[0],properties:ps,cid:cid,auth:auth}).then ((o) => {
                                return o
                              }))
            ao = (await senact('role:associate,cmd:add',
                               {sid:p.id,tid:d.id,cid:cid,auth:auth}).then ((o) => {
                                return o
                              }))
            for (let i = 1; i < rs.length; i++) {
              r = (await senact('role:relation,cmd:add',
                                {aid:ao.id,sid:p.id,type:rs[i],cid:cid,auth:auth}).then ((o) => {
                                  return o
                                }))
              ao.relations.push(r.id)
              d.rid.push(r.id)
              d.rtype.push(r.type)
	            info_cache.set(r.id,r)
            }

            ao.cid = cid
	          await senact('role:associate,cmd:upd',
	                       //{associate:ia}).then ((o) => {
	                       ao).then ((o) => {
	                         return o
	                       })
	          info_cache.set(ao.id,ao)
            d.cid = cid
	          await senact('role:descriptor,cmd:upd',
	                       //{descriptor:i}).then ((o) => {
	                       d).then ((o) => {
	                         return o
                         })
	          info_cache.set(d.id,d)
            p = (await senact('role:descriptor,cmd:get',
                              {id:p.id,cid:cid}).then ((o) => {
                                return o
                              }))
            info_cache.set(p.id,p)
            td = p
          }
          else {
            as[i] = decoded[c]+' @ '+new Date().toString().replace(/\.\w*/,'')
            d = (await senact('role:descriptor,cmd:push',
                              {pid:p.id,x:descriptor.x,y:descriptor.y,type:rs[0],properties:ps,cid:cid,auth:auth}).then ((o) => {
                                return o
                              }))
            td = d
          }

          ds.push(d)
          rs = []

          if (as.length > 0) {
            as.forEach((a,i) => {
              if (ns[i]) {
                if (a.includes(' @ '))
                  ns[i] = 'ts'
                ps.push({type:ts[i],name:ns[i],value:a})
              } else
                ps.push({type:ts[i],name:'value',value:a})
            })
            d.properties = ps
            d.type = descriptor.type
            d.cid = cid
	          await senact('role:descriptor,cmd:upd',
	                       //{descriptor:d}).then ((o) => {
	                       d).then ((o) => {
	                         return o
	                       })
            as = []
            ns = []
            ts = []
            ps = []
          }
          os.push(d)
          
          info_cache.set(d.id,d)

          if (decoded[c+1]=='p') {
            p = d
            pset = true
            ds = []
          }
          break
        case 'r':
          // relation
          rs.push(decoded[c])
          break
        case 'sr':
          // sibling relation
          rs.push(decoded[c])
          if (ds.length == 2) {
            ao = (await senact('role:associate,cmd:add',
                               {sid:ds[0].id,tid:ds[1].id,cid:cid,auth:auth}).then ((o) => {
                                 return o
                               }))
            ds[0].aid = ao.id
            ds[0].targets.push(ao.id)
            ds[1].aid = ao.id
            ds[1].sources.push(ao.id)
            for (let i = 0; i < rs.length; i++) {
              r = (await senact('role:relation,cmd:add',
                                {aid:ao.id,sid:ds[0].id,type:rs[i],cid:cid,auth:auth}).then ((o) => {
                                  return o
                                }))
              ao.relations.push(r.id)
              //ds[1].relations.push(r)
              ds[1].rid.push(r.id)
              ds[1].rtype.push(r.type)
	            info_cache.set(r.id,r)
            }

            ao.cid = cid
	          await senact('role:associate,cmd:upd',
	                       //{associate:ia}).then ((o) => {
	                       ao).then ((o) => {
	                         return o
	                       })
	          info_cache.set(ao.id,ao)
            ds[1].associate = ao
            ds[1].cid = cid
	          await senact('role:descriptor,cmd:upd',
	                       //{descriptor:i}).then ((o) => {
	                       ds[1]).then ((o) => {
	                         return o
                         })
	          info_cache.set(ds[1].id,ds[1])
            ds[0].cid = cid
            await senact('role:descriptor,cmd:upd',
                         ds[0]).then ((o) => {
                           return o
                         })
            info_cache.set(ds[0].id,ds[0])

            rs = []
            ds = []
          }
          break
        case 'up':
          d = (await senact('role:descriptor,cmd:get',
                            {id:p.id,cid:cid}).then ((o) => {
                              return o
                            }))
          p = (await senact('role:descriptor,cmd:get',
                            {id:d.pid,cid:cid}).then ((o) => {
                              return o
                            }))
          break

        default:
          console.dir('bad choice...:'+decoded[c+1])
        }
      }

      p = (await senact('role:descriptor,cmd:get',
                        {id:descriptor.pid,cid:cid}).then ((o) => {
                          return o
                        }))
      info_cache.set(p.id,p)

      if (td != null)
        reasonNodeAndDescendents(os,cid)

      /*
      end_ts = Date.now()
      let pts = pack(begin_ts, end_ts)

      console.dir(begin_ts+':'+end_ts+':'+pts)

      begin_ts = Math.floor(pts)
      let e_tm = 1/ (pts - begin_ts)
      end_ts = Math.round(begin_ts + e_tm)
      let quot = Math.floor(pts/ 3.1536e+14)
      begin_ts = pts - quot * 3.1536e+14
      end_ts = Math.floor((pts - begin_ts) / 3.1536e+14)
      let bts = pts % end_ts
      if (end_ts > begin_ts)
        console.dir(pts+'::'+begin_ts+'::'+end_ts)
      else
        console.dir('** '+pts+'::'+begin_ts+'::'+end_ts)
      */

      //gauges['descriptor.push.ts'].set({event:'descriptor.push', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address()}, pts)
      gauges['descriptor.push.ts'].set({event:'descriptor.push', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,os)
    } catch(e) {
      console.dir(e)
      //gauges['descriptor.push.ts'].set({event:'descriptor.push', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address()}, pts)
      gauges['descriptor.push.ts'].set({event:'descriptor.push', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.get', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.get.ts'])
      gauges['descriptor.get.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_get_ts',
        help: 'ts when getting a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth
      let d = null

      if (descriptor.did == 'metaroot') {
        if (!(d = info_cache.get(descriptor.did))) {
          d = (await senact('role:descriptor,cmd:get',
                            {id:descriptor.did,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(d.did, d)
        }
        /*
        d = info_cache.get(descriptor.did) ||
          (await senact('role:descriptor,cmd:getRoot',
                        {did:descriptor.did,cid:cid}).then ((o) => {
                          return o
                        }))
        */
        if (d == null) {
          d = (await senact('role:descriptor,cmd:addRoot',
                            {cid:cid,auth:auth}).then ((o) => {
                              return o
                            }))
        }

        info_cache.set(d.id,d)
      }
      else {
        if (!(d = info_cache.get(descriptor.id))) {
          d = (await senact('role:descriptor,cmd:get',
                            {id:descriptor.id,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(d.id, d)
        }
        /*
        d = info_cache.get(descriptor.id) ||
          (await senact('role:descriptor,cmd:get',
                        {id:descriptor.id,cid:cid}).then ((o) => {
                        //{id:descriptor.id,pid:descriptor.pid,x:descriptor.x,y:descriptor.y}).then ((o) => {
                          return o
                        }))
        */
        if (d == null) {
          let p = (await senact('role:descriptor,cmd:get',
                                {id:descriptor.pid,cid:cid}).then ((o) => {
                                  return o
                                }))
          if (p != null) {
            info_cache.set(p.id,p)
            d = (await senact('role:descriptor,cmd:add',
                              {id:descriptor.id,pid:descriptor.pid,type:p.type,x:descriptor.x,y:descriptor.y,cid:cid,auth:auth}).then ((o) => {
                                return o
                              }))
            info_cache.set(d.id,d)
          }
        }
        else
        if (!(auth.user == d.owner && d.perms & 256) &&
            !(d.group in auth.groups && d.perms & 32) &&
            !(d.perms & 4))
          return reply(null,null)
      }

      gauges['descriptor.get.ts'].set({event:'descriptor.get', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,d)
    } catch(e) {
      console.dir(e)
      gauges['descriptor.get.ts'].set({event:'descriptor.get', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:metaroot.rcv', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['metaroot.rcv.ts'])
      gauges['metaroot.rcv.ts'] = new client.Gauge({
        name: 'perf_mediator_metaroot_rcv_ts',
        help: 'ts when receiving a metaroot',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let m = {}
      let pl = []
      let cid = msg.cid
      let auth = msg.auth

      //info_cache.reset()

      let d
      if (!(d = (await senact('role:descriptor,cmd:getRoot',
                              {id:descriptor.did,cid:cid}).then ((o) => {
                                return o
                              })))) {
        if (!d)
          d = (await senact('role:descriptor,cmd:addRoot',
                            {cid:cid,auth:auth}).then ((o) => {
                              return o
                            }))
        info_cache.set(d.did, d)
      }

      if (d != null && 'id' in d) {
      //if (typeof d != 'undefined' && d != null && 'id' in d) {
        m.id = d.id
        m.did = d.did
        m.properties = d.properties
        m.targets = d.targets
        m.type = 'common'
      }

      gauges['metaroot.rcv.ts'].set({event:'metaroot.rcv', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,m)
    } catch(e) {
      console.dir(e)
      gauges['metaroot.rcv.ts'].set({event:'metaroot.rcv', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.rcv', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.rcv.ts'])
      gauges['descriptor.rcv.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_rcv_ts',
        help: 'ts when receiving a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth
      let m = {}
      m.targets = []
      let pl = []
      let did = descriptor.did
      let pid = descriptor.pid

      let p
      if (!(p = info_cache.get(pid))) {
        p = (await senact('role:descriptor,cmd:get',
                          {id:pid,cid:cid}).then ((o) => {
                            return o
                          }))
          info_cache.set(p.id, p)
      }
      /*
      let p = info_cache.get(pid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:pid,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      let targets = p.targets
      let rl = []
      let a, r, rm, relations, done, tid

      for (let i = 0; i < targets.length; i++) {
        if (!(a = info_cache.get(targets[i]))) {
          a = (await senact('role:associate,cmd:get',
                            {id:targets[i],cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(a.id, a)
        }
        /*
        a = info_cache.get(targets[i]) ||
          (await senact('role:associate,cmd:get',
                        {id:targets[i],cid:cid}).then ((o) => {
                          return o
                        }))
        */
        if (a != null && 'id' in a && a.id === did &&
            (a.owner == auth.user && a.perms & 256 ||
             a.group in auth.groups && a.perms & 32 ||
             a.perms & 4)) {
          let pm = {}
          pm.aid = a.id
          relations = a.relations
          for (let ii = 0; ii < relations.length; ii++) {
            if (!(r = info_cache.get(relations[ii]))) {
              r = (await senact('role:relation,cmd:get',
                                {id:relations[ii],cid:cid}).then ((o) => {
                                  return o
                                }))
              info_cache.set(r.id, r)
            }
            /*
            r = info_cache.get(relations[ii]) ||
              (await senact('role:relation,cmd:get',
                            {id:relations[ii],cid:cid}).then ((o) => {
                              return o
                            }))
            */
            if (r != null && 'id' in r &&
                (r.owner == auth.user && r.perms & 256 ||
                 r.group in auth.groups && r.perms & 32 ||
                 r.perms & 4)) {
              rm = {}
              rm.id = r.id
              rm.aid = a.id
              rm.type = r.type
              rl.push(rm)
            }
          }
          
          pm.rlist = rl
          pl.push(pm)
        }
      }

      done = false
      for (let i = 0; i < p.properties.length&&!done; i++) {
        if (p.properties[i].name == 'name') {
          m.pname = p.properties[i].value
          done = true
        }
      }
      if (!done) {
        m.pname = p.id
      }

      m.pid = pid
      m.prelations = pl
      if (!(a = info_cache.get(did))) {
        a = (await senact('role:associate,cmd:get',
                          {id:did,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(a.id, a)
      }
      /*
      a = info_cache.get(did) ||
        (await senact('role:associate,cmd:get',
                      {id:did,cid:cid}).then ((o) => {
                        return o
                      }))
      */
      if (a != null && 'id' in a &&
          (a.owner == auth.user && a.perms & 256 ||
           a.group in auth.groups && a.perms & 32 ||
           a.perms & 4)) {
        tid = a.tid
        let d
        if (!(d = info_cache.get(tid))) {
          d = (await senact('role:descriptor,cmd:get',
                            {id:tid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(d.id, d)
        }
        /*
        let d = info_cache.get(tid) ||
            (await senact('role:descriptor,cmd:get',
                          {id:tid,cid:cid}).then ((o) => {
                            return o
                          }))
        */
        m.id = tid
        m.targets = d.targets
        done = false
        for (let i = 0; i < d.properties.length&&!done; i++) {
          if (d.properties[i].name == 'name') {
            m.name = d.properties[i].value
            done = true
          }

          if (!done) {
            m.name = d.id
          }

          m.properties = d.properties
        }
      }
      
      gauges['descriptor.rcv.ts'].set({event:'descriptor.rcv', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,m)
    } catch (e) {
      console.dir(e)
      gauges['descriptor.rcv.ts'].set({event:'descriptor.rcv', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.drp', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.drp.ts'])
      gauges['descriptor.drp.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_drp_ts',
        help: 'ts when dropping a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth

      let d
      if (!(d = info_cache.get(descriptor.id))) {
        d = (await senact('role:descriptor,cmd:get',
                          {id:descriptor.id,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(d.id, d)
      }
      /*
      let d = info_cache.get(descriptor.id) ||
          (await senact('role:descriptor,cmd:get',
                        {id:descriptor.id,cid:cid}).then ((o) => {
                          return o
                        }))
      */
      if (!(auth.user == d.owner && d.perms & 128) &&
          !(d.group in auth.groups && d.perms & 16) &&
          !(d.perms & 2))
        return reply(null,null)
      
      let aid = null, a = null, did = null, td = null, tas = null, taid = null,
          ta = null, rs = null, rid = null
      let as = d.sources
	    let i = as.length

	    while (i--) {
	      aid = as[i]
        if (!(a = info_cache.get(aid))) {
          a = (await senact('role:associate,cmd:get',
                            {id:aid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(a.id, a)
        }
        /*
	      a = info_cache.get(aid) ||
	        (await senact('role:associate,cmd:get',
	                      {id:aid,cid:cid}).then ((o) => {
	                        return o
	                      }))
        */
	      did = a.sid
        if (!(td = info_cache.get(did))) {
          td = (await senact('role:descriptor,cmd:get',
                             {id:did,cid:cid}).then ((o) => {
                               return o
                             }))
          info_cache.set(td.id, td)
        }
        /*
	      td = info_cache.get(did) ||
	        (await senact('role:descriptor,cmd:get',
	                      {id:did,cid:cid}).then ((o) => {
	                          return o
	                        }))
        */
	      tas = td.targets
	      let ii = tas.length
	      while (ii--) {
	        taid = tas[ii]
	        if (taid === aid) {
	          td.targets.splice(ii,1)
	        }
	      }

        td.cid = cid
	      await senact('role:descriptor,cmd:upd',
	                   //{descriptor:td}).then ((o) => {
	                   td).then ((o) => {
	                     return o
	                   })
	      info_cache.set(td.id,td)

	      rs = a.relations
	      ii = rs.length

	      while (ii--) {
	        rid = rs[ii]
	        let r = await senact('role:relation,cmd:get',
	                             {id:rid,cid:cid}).then ((o) => {
	                               return o
	                             })
	        await senact('role:relation,cmd:drp',
	                     {id:rid,cid:cid}).then ((o) => {
	                       return o
	                     })
	        info_cache.delete(rid)
	        a.relations.splice(ii,1)
	      }

	      await senact('role:associate,cmd:drp',
	                   {id:aid,cid:cid}).then ((o) => {
	                     return o
	                   })
	      info_cache.delete(aid)
	      d.sources.splice(i,1)
	    }

	    as = d.targets
	    i = as.length
	    while (i--) {
	      aid = as[i]
        if (!(a = info_cache.get(aid))) {
          a = (await senact('role:associate,cmd:get',
                            {id:aid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(a.id, a)
        }
        /*
	      a = info_cache.get(aid) ||
	        (await senact('role:associate,cmd:get',
	                      {id:aid,cid:cid}).then ((o) => {
	                        return o
	                      }))
        */
        if (a != null &&
            a !== 'null' &&
            a !== 'undefined' &&
            typeof a !== 'null' &&
            typeof a !== 'undefined') {
	        did = a.tid
          if (!(td = info_cache.get(did))) {
            td = (await senact('role:descriptor,cmd:get',
                               {id:did,cid:cid}).then ((o) => {
                                 return o
                               }))
            info_cache.set(td.id, td)
          }
          /*
	        td = info_cache.get(did) ||
	          (await senact('role:descriptor,cmd:get',
	                        {id:did,cid:cid}).then ((o) => {
	                          return o
	                        }))
          */
	        tas = td.sources
	        let ii = tas.length
	        while (ii--) {
	          taid = tas[ii]
	          if (taid === aid) {
	            td.sources.splice(ii,1)
	          }
	        }

          td.cid = cid
	        await senact('role:descriptor,cmd:upd',
	                     //{descriptor:td}).then ((o) => {
	                     td).then ((o) => {
	                       return o
	                     })
	        info_cache.set(td.id,td)

	        rs = a.relations
	        ii = rs.length

	        while (ii--) {
	          rid = rs[ii]
	          let r = await senact('role:relation,cmd:get',
	                               {id:rid,cid:cid}).then ((o) => {
	                                 return o
	                               })
	          await senact('role:relation,cmd:drp',
	                       {id:rid,cid:cid}).then ((o) => {
	                         return o
	                       })
	          info_cache.delete(rid)
	          a.relations.splice(ii,1)
	        }

	        await senact('role:associate,cmd:drp',
	                     {id:aid,cid:cid}).then ((o) => {
	                       return o
	                     })
	        info_cache.delete(aid)
        }

        d.targets.splice(i,1)
	    }

	    info_cache.delete(descriptor.id)

	    await senact('role:descriptor,cmd:drp',
	                 {id:descriptor.id,cid:cid}).then ((o) => {
	                   return o
	                 })

      gauges['descriptor.drp.ts'].set({event:'descriptor.drp', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

	    return reply(null,{})
    } catch(e) {
      console.dir(e)
      gauges['descriptor.drp.ts'].set({event:'descriptor.drp', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:descriptor.rsn', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['descriptor.rsn.ts'])
      gauges['descriptor.rsn.ts'] = new client.Gauge({
        name: 'perf_mediator_descriptor_rsn_ts',
        help: 'ts when reasoning a descriptor',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let descriptor = msg.descriptor
      let cid = msg.cid
      let auth = msg.auth

      let d
      if (!(d = info_cache.get(descriptor.id))) {
        d = (await senact('role:descriptor,cmd:get',
                          {id:descriptor.id,cid:cid}).then ((o) => {
                            return o
                          }))
          info_cache.set(d.id, d)
      }
      /*
	    let d = info_cache.get(descriptor.id) ||
          (await senact('role:descriptor,cmd:get',
                        {id:descriptor.id,cid:cid}).then ((o) => {
                          return o
                        }))
      */
      if (!(auth.user == d.owner && d.perms & 64) &&
          !(d.group in auth.groups && d.perms & 8) &&
          !(d.perms & 1))
        return reply(null,[])

      await senact('role:reason,cmd:reason',
                   {id:d.id,cid:cid}).then ((o) => {
                     return o
                   })

      gauges['descriptor.rsn.ts'].set({event:'descriptor.rsn', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,[d])
    } catch(e) {
      console.dir(e)
      gauges['descriptor.rsn.ts'].set({event:'descriptor.rsn', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:properties.upd', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['properties.upd.ts'])
      gauges['properties.upd.ts'] = new client.Gauge({
        name: 'perf_mediator_properties_upd_ts',
        help: 'ts when upding properties',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let properties = msg.properties
      let cid = msg.cid

      properties.cid = cid
      let d = (await senact('role:properties,cmd:upd',
                            //{properties:properties}).then ((o) => {
                            properties).then ((o) => {
                              return o
                            }))
      info_cache.set(d.id,d)
      gauges['properties.upd.ts'].set({event:'properties.upd', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,[d])
    } catch(e) {
      console.dir(e)
      gauges['properties.upd.ts'].set({event:'properties.upd', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:relation.add', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['relation.add.ts'])
      gauges['relation.add.ts'] = new client.Gauge({
        name: 'perf_mediator_relation_add_ts',
        help: 'ts when adding a relation',
          labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let properties = msg.relation
      let aid = properties.aid
      let cid = msg.cid
      let auth = msg.auth
      let a = null

      if (aid == 'taid') {
        let sd
        if (!(sd = info_cache.get(properties.sid))) {
          sd = (await senact('role:descriptor,cmd:get',
                             {id:properties.sid,cid:cid}).then ((o) => {
                               return o
                            }))
          info_cache.set(sd.id, sd)
        }
        /*
        let sd = info_cache.get(properties.sid) ||
            (await senact('role:descriptor,cmd:get',
                          {id:properties.sid,cid:cid}).then ((o) => {
                            return o
                          }))
        */
        let td
        if (!(td = info_cache.get(properties.tid))) {
          td = (await senact('role:descriptor,cmd:get',
                             {id:properties.tid,cid:cid}).then ((o) => {
                               return o
                             }))
          info_cache.set(td.id, td)
        }
        /*
          let td = info_cache.get(properties.tid) ||
          (await senact('role:descriptor,cmd:get',
          {id:properties.tid,cid:cid}).then ((o) => {
          return o
          }))
        */

        if (!(auth.user == sd.owner && sd.perms & 128) &&
            !(sd.group in auth.groups && sd.perms & 16) &&
            !(sd.perms & 2))
          return reply(null,null)
        if (!(auth.user == td.owner && td.perms & 128) &&
            !(td.group in auth.groups && td.perms & 16) &&
            !(td.perms & 2))
          return reply(null,null)

        a = (await senact('role:associate,cmd:add',
                          {sid:properties.sid,tid:properties.tid,cid:cid,auth:auth}).then ((o) => {
                            return o
                          }))

        info_cache.set(a.id,a)
        sd.targets.push(a.id)
        sd.cid = cid
        await senact('role:descriptor,cmd:upd',
                     //{descriptor:d}).then ((o) => {
                     sd).then ((o) => {
                       return o
                     })
        info_cache.set(sd.id,sd)
        td.sources.push(a.id)
        td.cid = cid
        await senact('role:descriptor,cmd:upd',
                     //{descriptor:d}).then ((o) => {
                     td).then ((o) => {
                       return o
                     })
        info_cache.set(td.id,td)
      }
      else {
        if (!(a = info_cache.get(properties.aid))) {
          a = (await senact('role:associate,cmd:get',
                            {id:properties.aid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(a.id, a)
        }
        /*
        a = info_cache.get(properties.aid) ||
          (await senact('role:associate,cmd:get',
                        {id:properties.aid,cid:cid}).then ((o) => {
                          return o
                        }))
        */
      }

      if (!(auth.user == a.owner && a.perms & 128) &&
          !(a.group in auth.groups && a.perms & 16) &&
          !(a.perms & 2))
        return reply(null,null)

      let r = (await senact('role:relation,cmd:add',
                            {aid:a.id,sid:properties.sid,type:'describes',cid:cid,auth:auth}).then ((o) => {
                              return o
                            }))
      info_cache.set(r.id,r)
      a.relations.push(r.id)
      a.cid = cid
      await senact('role:associate,cmd:upd',
                   //{associate:a}).then ((o) => {
                   a).then ((o) => {
                     return o
                   })
      info_cache.set(a.id,a)

      gauges['relation.add.ts'].set({event:'relation.add', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid: cid}, pack(begin_ts, Date.now()))

      return reply(null,r)
    } catch(e) {
      console.dir(e)
      gauges['relation.add.ts'].set({event:'relation.add', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:relation.get', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['relation.get.ts'])
      gauges['relation.get.ts'] = new client.Gauge({
        name: 'perf_mediator_relation_get_ts',
        help: 'ts when getting a relation',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let relation = msg.relation
      let cid = msg.cid
      let auth = msg.auth

      let r
      if (!(r = info_cache.get(relation.id))) {
        r = (await senact('role:relation,cmd:get',
                          {id:relation.id,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(r.id, r)
      }
      /*
      let r = info_cache.get(relation.id) ||
          (await senact('role:relation,cmd:get',
                        {id:relation.id,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      if (!(auth.user == r.owner && r.perms & 256) &&
          !(r.group in auth.groups && r.perms & 32) &&
          !(r.perms & 4))
        return reply(null,null)

      /*
      if ( r.length==0 ) {
        r = (await senact('role:relation,cmd:add',
                          {id:relation.id}).then ((o) => {
                            return o
                          }))
        info_cache.set(r.id,r)
      }
      */

      gauges['relation.get.ts'].set({event:'relation.get', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,r)
    } catch(e) {
      console.dir(e)
      gauges['relation.get.ts'].set({event:'relation.get', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:relation.upd', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['relation.upd.ts'])
      gauges['relation.upd.ts'] = new client.Gauge({
        name: 'perf_mediator_relation_upd_ts',
        help: 'ts when updating a relation',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let relation = msg.relation
      let cid = msg.cid
      let auth = msg.auth

      relation.cid = cid
      relation.auth = auth
      let r = (await senact('role:relation,cmd:upd',
                            //{id:relation.id,type:relation.type,cid:relation.id,auth:msg.auth}).then ((o) => {
                            relation).then ((o) => {
                              return o
                            }))

      info_cache.set(r.id,r)

      gauges['relation.upd.ts'].set({event:'relation.upd', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,r)
    } catch(e) {
      console.dir(e)
      gauges['relation.upd.ts'].set({event:'relation.upd', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:relation.drp', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['relation.drp.ts'])
      gauges['relation.drp.ts'] = new client.Gauge({
        name: 'perf_mediator_relation_drp_ts',
        help: 'ts when dropping a relation',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let relation = msg.relation
      let cid = msg.cid
      let auth = msg.auth

      let r
      if (!(r = info_cache.get(relation.id))) {
        r = (await senact('role:relation,cmd:get',
                          {id:relation.id,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(r.id, r)
      }
      /*
      let r = info_cache.get(relation.id) ||
          (await senact('role:relation,cmd:get',
                        {id:relation.id,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      if (!(auth.user == r.owner && r.perms & 128) &&
          !(r.group in auth.groups && r.perms & 16) &&
          !(r.perms & 2))
        return reply(null,null)

      let a
      if (!(a = info_cache.get(r.aid))) {
        a = (await senact('role:associate,cmd:get',
                          {id:r.aid,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(a.id, a)
      }
      /*
      let a = info_cache.get(r.aid) ||
          (await senact('role:associate,cmd:get',
                        {id:r.aid,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      let ii = a.relations.length
      while (ii--) {
        if (a.relations[ii]===r.id) {
          a.relations.splice(ii,1)
          break
        }
      }

      if (!a.relations.length) {
        let d
        if (!(d = info_cache.get(a.sid))) {
          d = (await senact('role:descriptor,cmd:get',
                            {id:a.sid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(d.id, d)
        }
        /*
        let d = info_cache.get(a.sid) ||
            (await senact('role:descriptor,cmd:get',
                          {id:a.sid,cid:cid}).then ((o) => {
                            return o
                          }))
        */
        let as = d.targets
        let i = as.length
        while (i--)
          if (as[i] === a.id)
            d.targets.splice(i,1)
        d.cid = cid
        await senact('role:descriptor,cmd:upd',
                     //{descriptor:d}).then ((o) => {
                     d).then ((o) => {
                       return o
                     })
        info_cache.set(d.id,d)
        if (!(d = info_cache.get(a.tid))) {
          d = (await senact('role:descriptor,cmd:get',
                            {id:a.tid,cid:cid}).then ((o) => {
                              return o
                            }))
          info_cache.set(d.id, d)
        }
        /*
        d = info_cache.get(a.tid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:a.tid,cid:cid}).then ((o) => {
                          return o
                        }))
        */
        as = d.sources
        i = as.length
        while (i--)
          if (as[i] === a.id)
            d.sources.splice(i,1)
        d.cid = cid
        await senact('role:descriptor,cmd:upd',
                     //{descriptor:d}).then ((o) => {
                     d).then ((o) => {
                       return o
                     })
        info_cache.set(d.id,d)
        await senact('role:associate,cmd:drp',
                     {id:a.id,cid:cid}).then ((o) => {
                       return o
                     })
        info_cache.delete(a.id)
      }
      else {
        a.cid = cid
        await senact('role:associate,cmd:upd',
                     //{associate:a}).then ((o) => {
                     a).then ((o) => {
                       return o
                     })
        info_cache.set(a.id,a)
      }

      info_cache.delete(relation.id)
      await senact('role:relation,cmd:drp',
                   {id:relation.id,cid:cid}).then ((o) => {
                     return o
                   })

      gauges['relation.drp.ts'].set({event:'relation.drp', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))
      return reply(null,{})
    } catch(e) {
      console.dir(e)
      gauges['relation.drp.ts'].set({event:'relation.drp', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  Seneca.add('role:mediator,cmd:associate.add', async (msg, reply) => {

    let begin_ts = Date.now()

    if (!gauges['associate.add.ts'])
      gauges['associate.add.ts'] = new client.Gauge({
        name: 'perf_mediator_associate_add_ts',
        help: 'ts when adding an associate',
        labelNames: ['event','return_code','service','cluster','app','user','ip','cid'],
        registers: [registry]
      })

    try {
      let Seneca = this
      let associate = msg.associate
      let cid = msg.cid
      let auth = msg.auth

      let sd
      if (!(sd = info_cache.get(association.sid))) {
        sd = (await senact('role:descriptor,cmd:get',
                          {id:association.sid,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(sd.id, sd)
      }
      /*
      let sd = info_cache.get(associate.sid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:associate.sid,cid:cid}).then ((o) => {
                          return o
                        }))
      */
      let td
      if (!(td = info_cache.get(association.tid))) {
        td = (await senact('role:descriptor,cmd:get',
                          {id:association.tid,cid:cid}).then ((o) => {
                            return o
                          }))
        info_cache.set(td.id, td)
      }
      /*
      let td = info_cache.get(associate.tid) ||
          (await senact('role:descriptor,cmd:get',
                        {id:associate.tid,cid:cid}).then ((o) => {
                          return o
                        }))
      */

      if (!(auth.user == sd.owner && sd.perms & 128) &&
          !(sd.group in auth.groups && sd.perms & 16) &&
          !(sd.perms & 2))
        return reply(null,null)
      if (!(auth.user == td.owner && td.perms & 128) &&
          !(td.group in auth.groups && td.perms & 16) &&
          !(td.perms & 2))
        return reply(null,null)

        a = (await senact('role:associate,cmd:add',
                          {sid:associate.sid,tid:associate.tid,cid:cid,auth:auth}).then ((o) => {
                            return o
                          }))

      info_cache.set(a.id,a)
      sd.targets.push(a.id)
      sd.cid = cid
      await senact('role:descriptor,cmd:upd',
                   //{descriptor:d}).then ((o) => {
                   sd).then ((o) => {
                     return o
                   })
      info_cache.set(sd.id,sd)
      td.sources.push(a.id)
      td.cid = cid
      await senact('role:descriptor,cmd:upd',
                   //{descriptor:d}).then ((o) => {
                   td).then ((o) => {
                     return o
                   })
      info_cache.set(td.id,td)

      gauges['associate.add.ts'].set({event:'associate.add', return_code:'200', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:cid}, pack(begin_ts, Date.now()))

      return reply(null,a)
    } catch(e) {
      console.dir(e)
      gauges['associate.add.ts'].set({event:'associate.add', return_code:'500', service:'mediator', cluster:process.env.cluster, app:process.env.app, user:process.env.user, ip:ip.address(), cid:{}}, pack(begin_ts, Date.now()))
    }
  })

  /*
  Seneca.add('role:mediator,cmd:associate.get', async (msg, reply) => {
    try {
      let Seneca = this
      let associate = msg.associate
      let a = info_cache.get(associate.id) ||
          (await senact('role:associate,cmd:get',
                        associate).then ((o) => {
                          return o
                        }))
      if (a.length==0) {
        a = (await senact('role:associate,cmd:add',
                          associate).then ((o) => {
                            return o
                          }))
        info_cache.set(a.id,a)
      }

      return reply(null,a)
    } catch(e) {
      console.dir(e)
    }
  })

  Seneca.add('role:mediator,cmd:associate.upd', async (msg, reply) => {
    let Seneca = this
    let associate = msg.associate
    let a = (await senact('role:associate,cmd:upd',
                          {obj:associate}).then ((o) => {
                            return o
                          }))
    info_cache.set(a.id,a)
    return reply(null,a)
  })
  */
}
