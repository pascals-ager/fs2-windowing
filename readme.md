

## Author's Readme:
##Assumptions:

- TransactionWindow maintains a cache of Transactions for the last     
  'windowSize' duration.  The implementation uses event     
  processingTime (arrival time) rather than event transaction time     
  (event time).   What that means is that a Transaction with event     
  time: "2019-02-13T10:00:00.000Z" and another with event time:      
  "2019-02-13T11:00:00.000Z",  although have one hour between their     
  event times; will belong to the same window of size 2 minutes, if     
  they arrive/are-seen by the streaming system without their natural    
  delay.

- frequencyTolerance is the number of transactions per windowSize that are tolerated by the TransactionWindow
- doubledTolerance is the number of transaction for merchant, amount per windowSize that are tolerated by the
*  TransactionWindow

- TransactionWindow models the underlying window as HashMap where:    K  
  -> Combination of Transaction merchant and amount.  This can be replaced by any Transaction => String to optimize for common lookup  
  operations which require  O(1) performance. For simpler design the  
  function used for K is Transaction =>  
  Transaction.merchant+Transaction.amount.   This sufficiently models  
  the use case. *  V -> List[(transactionTime, processingTime)].  New  
  transactions for the same key are appended to the list with their  
  timestamp metadata as the value  Eviction -> Evict K entries and  
  Evict `processingTime` timestamp entries in V after `windowSize`  
  expiry period.

#### How to build and Test
1. Test: simply clone the repository and run `sbt test`
 ```  
 Running the tests might take a while because I introduced sleeps to test for cache eviction. This is not ideal but I would liked to have solved it with monix TestScheduler given more time. My intial attempts to use the TestScheduler to simulate time ticks were not successful because the eviction is strictly    
 time based. Ticking time does not tick it on the window eviction fiber, and the TestScheduler does not provide an interface for fixed delay schedules.  
 ```  
2. Build: This repo use a native-packager to package this application into a Docker Image. To build the docker image:    
   `sbt docker:publishLocal` publishes `authorizerfs2:0.1`

3. To run locally: `cat operations |  docker run -i  authorizerfs2:0.1 /bin/bash -c 'cat'`
```  
{  
  "account" : {  
    "active-card" : true,  
    "available-limit" : 100  
  },  
  "violations" : [  
  ]  
}  
{  
  "account" : {  
    "active-card" : true,  
    "available-limit" : 80  
  },  
  "violations" : [  
  ]  
}  
{  
  "account" : {  
    "active-card" : true,  
    "available-limit" : 80  
  },  
  "violations" : [  
    "insufficient-limit"  
  ]  
}  
  
  
```  
This is not ideal to simulate delay between events. But one can write an events generator to pipe events to operations. This event generator can simulate delay between events

4. Configs are loaded from docker environment variables. The default variables are set in `build.sbt` file.   These can be be overriden runtime.
```  
dockerEnvVars := Map(  
  "TIME_WINDOW_SIZE_SECONDS" -> "120",  
  "TOPIC_QUEUE_SIZE" -> "10",  
  "TRANSACTION_FREQUENCY_TOLERANCE" -> "3",  
  "TRANSACTION_DOUBLED_TOLERANCE" -> "1"  
) 
```  

Additionally, given more time I would have liked to
1) Write more async tests to test against invariants
2) Find a way to deal with event transactionTime rather than processing time
3) Generalise TransactionWindow for any `T`