Overview
===========

A cross platform event server based on socket io.  
A .NET client is now provided.

Installation
===========

```bash
git clone https://github.com/zhangyaoxing/SocketEvent
cd SocketEvent
npm install
```

Configuration
===========
Config file locates in /config/default.json
```javascript
 {  
   "connections": {
     "url": "mongodb://127.0.0.1/SocketEvent" // database connection string
   },
   "waitings": ["MerchantServiceClient"], // waiting for these clients before firing any event.
   "queueCollectionName": "queue" // don't change this line
 }
```
Run
===========
> node app.js
