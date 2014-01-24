async = require 'async'
chai = require 'chai'
mongodb = require 'mongodb'
ObjectID = require('mongodb').ObjectID

should = chai.should()

committed = require './../committed'

_db = null
dbconfig =
    server   : "127.0.0.1"
    port     : "27017"
    database : "committedtest"

before (done) ->
    committed.register 'db', committed.db
    failMethod = (config, transaction, state, args, done) ->
        done(null, false)
    committed.register 'failMethod', failMethod
    committed.register 'failMethodRollback', committed.db.pass

    errorMethod = (config, transaction, state, args, done) ->
        done 'Supposed to fail', false
    committed.register 'errorMethod', errorMethod
    committed.register 'errorMethodRollback', committed.db.pass

    blockingMethod = (config, transaction, state, args, done) ->
        setTimeout (()-> done(null, true)), 1000
    committed.register 'blockingMethod', blockingMethod
    committed.register 'blockingMethodRollback', committed.db.pass
    
    # Connect to the database
    dbaddress = 'mongodb://' + dbconfig.server + '/' + dbconfig.database

    mongodb.MongoClient.connect dbaddress, (err, db) ->
        if err? then done(err)
        else
            _db = db
            done()

describe 'Committed', ->

    describe 'start', ->

        it 'should start without error', (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                done err

        it 'should not start a second time', (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                err.message.should.have.string 'committed has already been started'
                done()

        it 'should stop without error only if started', (done) ->
            committed.stop (err) ->
                should.not.exist err
                committed.stop (err) ->
                    err.message.should.have.string 'committed is not currently started'
                    done()

        it 'should accept the predefined named functions (once)', (done) ->
            try
                committed.register 'db', committed.db
            catch error
                error.message.should.have.string 'path db already exists in registry'
            finally
                done()

        it 'should not be possible to duplicate a registered function name', (done) ->
            try
                committed.register 'db.insert', 'foo'
            catch error
                error.message.should.have.string 'path db.insert already exists in registry'
            finally
                done()


    describe 'valid operation', ->

        before (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'validOpsTest', done

        after (done) ->
            committed.stop done

        it 'should be able to sequentially process multiple operations in a single queue', (done) ->
            # Create an empty collection
            # Queue up 100 inserts into a collection
            # Go!
            # is it much slower than doing it directly?

            task = (n, next) ->
                transaction = committed.transaction()
                transaction.queue = 'testQ'
                transaction.instructions.push
                    name: 'db.insert'
                    arguments: ['validOpsTest', {value: n + 10}]
                committed.enqueue transaction, next

            start = new Date().getTime()

            async.times 20, task, (err) ->
                transactionTime = new Date().getTime() - start
                should.not.exist err

                task = (n, next) ->
                    _db.collection('validOpsTest').insert {value: n + 20}, {w:1, journal: true, upsert:false, multi: false, serializeFunctions: false}, next

                start = new Date().getTime()
                async.timesSeries 20, task, (err) ->
                    should.not.exist err
                    directTime = new Date().getTime() - start
                    #we think a transactioned insert should be approximately 4 times slower than direct one
                    (transactionTime / directTime).should.be.below 5
                    done()

        it 'should be possible to place transactions in multiple queues', (done) ->
            # Push 10 transactions into each of 10 queues

            tasks = []
            for t in [1..10]
                do (t) ->
                    for q in [1..10]
                        do (q) ->
                            tasks.push (done) ->
                                transaction = committed.transaction "testQ#{q}", 'user', [
                                    name: 'db.insert', arguments: ['validOpsTest', {t:t, q:q}]
                                ]
                                committed.enqueue transaction, (err, status) ->
                                    should.not.exist err
                                    status.should.be.string 'Committed'
                                    done()

            async.parallel tasks, done


    describe 'global lock', ->

        before (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'globalLockTest', done

        after (done) ->
            committed.stop done

        it 'should let normal queues process after a GlobalLock transaction', (done) ->
            globalTransaction = committed.transaction 'GlobalLock', 'user'
            globalTransaction.instructions.push
                name: 'blockingMethod'
            committed.withGlobalLock globalTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'

                # after the global lock transaction is finished, make sure the queues are open again
                nonGlobalTransaction = committed.transaction 'nonGlobal', 'user', [], []
                committed.enqueue nonGlobalTransaction, (err, status) ->
                    status.should.equal 'Committed'
                    done(err)

        it 'should let immediate transactions process after a GlobalLock transaction', (done) ->
            globalTransaction = committed.transaction 'GlobalLock', 'user', [
                name: 'blockingMethod'
            ]
            committed.withGlobalLock globalTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'

                immediateTransaction = committed.transaction()
                committed.immediately immediateTransaction, (err, status) ->
                    status.should.equal 'Committed'
                    done(err)

        it 'should wait for transactions in other queues to finish before starting a global lock transaction', (done) ->
            d = new Date()
            doc1 = now: d; doc2 = now: d; doc3 = now: d
            ot1 = committed.transaction 'q1', 'user', [
                {name: 'blockingMethod'}
                {name: 'db.insert', arguments: ['globalLockTest', doc1]}
            ]
            ot2 = committed.transaction 'q2', 'user', [
                {name: 'blockingMethod'}
                {name: 'db.insert', arguments: ['globalLockTest', doc2]}
            ]
            ot3 = committed.transaction 'q3', 'user', [
                {name: 'blockingMethod'}
                {name: 'db.insert', arguments: ['globalLockTest', doc3]}
            ]
            globalLockTestMethod = (config, transaction, state, args, instructionDone) ->
                config.db.collection 'globalLockTest', {strict:true}, (err, collection) ->
                    if err? then return instructionDone(err)
                    collection.find({now: d}).toArray (err, docs) ->
                        #these ensure that the insert has already happened,
                        #and hence that our global transaction has correctly
                        #waited
                        docs.length.should.equal 3
                        instructionDone(null, true)
            committed.register 'globalLockTestMethod', globalLockTestMethod
            committed.register 'globalLockTestMethodRollback', committed.db.pass
            gt = committed.transaction 'GlobalLock', 'user', [
                {name: 'globalLockTestMethod'} 
            ]

            committed.enqueue ot1, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue ot2, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue ot3, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                done()

        it 'should wait for immediate transactions to complete before starting a global lock', (done) ->
            doc = now: new Date()
            im = committed.transaction null, 'user', [
                {name: 'blockingMethod'}
                {name: 'db.insert', arguments: ['globalLockTest', doc]}
            ]
            globalImmediateTestMethod = (config, transaction, state, args, instructionDone) ->
                config.db.collection 'globalLockTest', {strict:true}, (err, collection) ->
                    if err? then return instructionDone(err)
                    collection.find(doc).toArray (err, docs) ->
                        #these ensure that the insert has already happened,
                        #and hence that our global transaction has correctly
                        #waited
                        docs.length.should.equal 1
                        docs[0].now.should.deep.equal doc.now
                        instructionDone(null, true)
            committed.register 'globalImmediateTestMethod', globalImmediateTestMethod
            committed.register 'globalImmediateTestMethodRollback', committed.db.pass
            gt = committed.transaction 'GlobalLock', 'user', [ name: 'globalImmediateTestMethod' ]
            committed.immediately im, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                done()

        it 'should pend ordinary queues during globallock and resume them after', (done) ->
            d = new Date()
            doc1 = now: d; doc2 = now: d; doc3 = now: d
            gt1 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc1]}
            ]
            gt2 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc2]}
            ]
            gt3 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc3]}
            ]
            pendingTestMethod = (config, transaction, state, args, instructionDone) ->
                config.db.collection 'globalLockTest', {strict:true}, (err, collection) ->
                    if err? then return instructionDone(err)
                    collection.find({now: d}).toArray (err, docs) ->
                        #these ensure that the insert has already happened,
                        #and hence that our global transaction has correctly
                        #waited
                        docs.length.should.equal 3
                        instructionDone(null, true)
            committed.register 'pendingTestMethod', pendingTestMethod
            committed.register 'pendingTestMethodRollback', committed.db.pass
            ot = committed.transaction 'Q', 'user', [
                {name: 'pendingTestMethod'} 
            ]

            committed.withGlobalLock gt1, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt2, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt3, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue ot, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                done()

        it 'should pend immediately transactions during globallock and resume after', (done) ->
            d = new Date()
            doc1 = now: d; doc2 = now: d; doc3 = now: d
            gt1 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc1]}
            ]
            gt2 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc2]}
            ]
            gt3 = committed.transaction 'GlobalLock', 'user', [
                {name: 'db.insert', arguments: ['globalLockTest', doc3]}
            ]
            pendingImmediateTestMethod = (config, transaction, state, args, instructionDone) ->
                config.db.collection 'globalLockTest', {strict:true}, (err, collection) ->
                    if err? then return instructionDone(err)
                    collection.find({now: d}).toArray (err, docs) ->
                        #these ensure that the insert has already happened,
                        #and hence that our global transaction has correctly
                        #waited
                        docs.length.should.equal 3
                        instructionDone(null, true)
            committed.register 'pendingImmediateTestMethod', pendingImmediateTestMethod
            committed.register 'pendingImmediateTestMethodRollback', committed.db.pass
            im = committed.transaction null, 'user', [
                {name: 'pendingTestMethod'} 
            ]

            committed.withGlobalLock gt1, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt2, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt3, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.immediately im, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                done()            

    describe 'stop', ->

        beforeEach (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'stopTest', (err, collection) -> 
                    if err? then return done(err)
                    collection.remove {}, {w:1}, done

        afterEach (done) ->
            committed.stop done

        it 'should wait for immediate, enqueued, and global lock tasks to complete before stopping', (done) ->
            doc1 = inserted: 'beforeStop'
            doc2 = inserted: 'beforeStop'
            doc3 = inserted: 'beforeStop'
            im = committed.transaction null, 'user', [ {name: 'db.insert', arguments: ['stopTest', doc1]} ]
            gt = committed.transaction 'GlobalLock', 'user', [{name: 'db.insert', arguments: ['stopTest', doc2]}]
            ot = committed.transaction 'OrdinaryQueue', 'user', [{name: 'db.insert', arguments: ['stopTest', doc3]}]
            committed.immediately im, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue ot, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.withGlobalLock gt, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            # setTimeout (()-> done()), 1000
            committed.stop (err) ->
                should.not.exist err
                _db.collection 'stopTest', {strict:true}, (err, collection) ->
                    collection.find({inserted: 'beforeStop'}).toArray (err, docs) ->
                        docs.length.should.equal 3
                        done()


    describe 'rollback', ->

        before (done) ->
            failSlowlyMethod = (config, transaction, state, args, done) ->
                setTimeout (() -> done(null, false)), 1000
            committed.register 'failSlowlyMethod', failSlowlyMethod
            
            failSlowlyMethodRollback = (config, transaction, state, args, done) ->
                setTimeout (() -> done(null, true)), 1000
            committed.register 'failSlowlyMethodRollback', failSlowlyMethodRollback
            done()

        beforeEach (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'rollbackTest', (err, collection) ->
                    collection.remove {}, {w:1}, done

        afterEach (done) ->
            committed.stop done

        it 'should rollback an errored transaction and return status FailedCommitErrorRollbackOk', (done) ->
            docA = {foo:true}
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', docA]}
                {name: 'errorMethod'}
            ], [
                {name: 'db.insertRollback', arguments: ['rollbackTest', docA]}
                {name: 'db.pass'}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.equal 'FailedCommitErrorRollbackOk'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count docA, (err, count) ->
                    count.should.equal 0
                    done(err)

        it 'should not execute rollbacks for instructions which were not executed, nor execute further instructions after an error', (done) ->
            docA = {foo:true}
            docB = {bar:true}
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', docA]}
                {name: 'errorMethod'}
                {name: 'db.insert', arguments: ['rollbackTest', docB]}
            ], [
                {name: 'db.insertRollback', arguments: ['rollbackTest', docA]}
                {name: 'db.pass'}
                {name: 'errorMethod'}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                #rollback is okay because we never executed the final rollback error method
                status.should.equal 'FailedCommitErrorRollbackOk'
                #and check that we both rolled back the first insert properly
                _db.collection('rollbackTest').count {_id: $in: [docA._id, docB._id]}, (err, count) ->
                    count.should.equal 0
                    done(err)


        it 'should rollback a failed transaction and return status Failed', (done) ->
            doc = {rolledback: false}
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', doc]}
                {name: 'failMethod'}
            ], [
                {name: 'db.updateOneOp', arguments: ['rollbackTest', doc, {__set: rolledback: true}, {}]}
                {name: 'db.pass'}
            ]
            committed.enqueue transaction, (err, status) ->
                status.should.equal 'Failed'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count {_id: doc._id, rolledback: true}, (err, count) ->
                    count.should.equal 1
                    done(err)

        it 'should report a catastrophe if a rollback fails after an error', (done) ->
            doc = content: 'agogo'
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', doc]}
                {name: 'errorMethod'} #this will error the transaction
            ], [
                {name: 'errorMethod'}
                {name: 'errorMethod'} 
            ]
            committed.enqueue transaction, (err, status) ->
                status.should.equal 'CatastropheCommitErrorRollbackError'
                err.should.equal 'Supposed to fail'
                done()

        it 'should report a catastrophe if a rollback fails after a failure', (done) ->
            doc = content: 'agogo'
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', doc]}
                {name: 'failMethod'}
            ], [
                {name: 'failMethod'}
                {name: 'failMethod'}
            ]
            committed.enqueue transaction, (err, status) ->
                status.should.equal 'CatastropheCommitFailedRollbackError'
                done(err)

        it 'should use implicit rollbacks when the transaction.rollback array is null', (done) ->
            transaction = committed.transaction 'rollbackTest', 'user', [
                {name: 'db.insert', arguments: ['rollbackTest', {content: 'great content'}]}
                {name: 'db.insert', arguments: ['rollbackTest', {content: 'more great content'}]}
                {name: 'failSlowlyMethod', arguments: []}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Failed'
                _db.collection('rollbackTest').find().toArray (err, docs) ->
                    docs.length.should.equal 0
                    done() 


    describe 'error handling', ->

        before (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, done

        after (done) ->
            committed.stop done

        it 'should reject a transaction for processing with an invalid queue name', (done) ->

            tests = []

            tests.push (done) ->
                globalTransaction = committed.transaction 'GlobalLock'
                committed.enqueue globalTransaction, (err) ->
                    err.message.should.have.string "Can't queue a transaction for GlobalLock using the committed.enqueue function"
                    done()

            tests.push (done) ->
                nullQueueTransaction = committed.transaction null
                committed.enqueue nullQueueTransaction, (err) ->
                    err.message.should.have.string 'must have a transaction.queue parameter to use committed.enqueue'
                    done()

            tests.push (done) ->
                emptyQueueTransaction = committed.transaction ''
                committed.enqueue emptyQueueTransaction, (err) ->
                    err.message.should.have.string 'must have a transaction.queue parameter to use committed.enqueue'
                    done()

            tests.push (done) ->
                nonGlobalTransaction = committed.transaction 'testQ'
                committed.withGlobalLock nonGlobalTransaction, (err) ->
                    err.message.should.have.string "Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"
                    done()

            tests.push (done) ->
                immediateTransaction = committed.transaction 'testQ'
                committed.immediately immediateTransaction, (err) ->
                    err.message.should.have.string "Can't call committed.immediately on a transaction which has a queue defined for it"
                    done()

            async.parallel tests, done


        it 'should fail a transaction if not started', (done) ->

            committed.stop (err) ->
                should.not.exist err

                tests = []

                tests.push (done) ->
                    transaction = committed.transaction 'testQ'
                    committed.enqueue transaction, (err, status) ->
                        should.not.exist err
                        status.should.have.string 'Failed'
                        done()

                tests.push (done) ->
                    transaction = committed.transaction()
                    committed.immediately transaction, (err, status) ->
                        should.not.exist err
                        status.should.have.string 'Failed'
                        done()

                tests.push (done) ->
                    transaction = committed.transaction 'GlobalLock'
                    committed.withGlobalLock transaction, (err, status) ->
                        should.not.exist err
                        status.should.have.string 'Failed'
                        done()

                async.parallel tests, (err) ->
                    if err? then return done(err)
                    committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, done


    describe 'instructions', ->

        beforeEach (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'instructionsTest', (err, collection) -> 
                    collection.remove {}, {w:1}, done

        afterEach (done) ->
            committed.stop done

        it 'insert should reach mongo, and be issued with an _id', (done) ->
            doc = {t:'t', q:'q'}
            transaction = committed.transaction "test", 'user', [
                {name: 'db.insert', arguments: ['instructionsTest', doc]}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                should.exist(doc._id)
                _db.collection 'instructionsTest', (err, collection) ->
                    collection.findOne {_id: doc._id}, (err, docInDB) ->
                        doc.t.should.equal docInDB.t
                        doc.q.should.equal docInDB.q
                        done()

        it 'insert should work with multiple documents', (done) ->
            docs = ( {i: i} for i in [0..10] )
            transaction = committed.transaction "test", 'user', [
                {name: 'db.insert', arguments: ['instructionsTest', docs]}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                _db.collection 'instructionsTest', (err, collection) ->
                    ids = ( doc._id for doc in docs )
                    collection.find( _id: $in: ids ).toArray (err, docsInDB) ->
                        docsInDB.length.should.equal docs.length
                        done()

        it 'insertRollback should undo insert for multiple documents', (done) ->
            docs = ( {i: i} for i in [0..10] )
            transaction = committed.transaction "test", 'user', [
                {name: 'db.insert', arguments: ['instructionsTest', docs]}
                {name: 'failMethod'}
            ], [
                {name: 'db.insertRollback', arguments: ['instructionsTest', docs]}
                {name: 'db.pass'}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Failed'
                _db.collection 'instructionsTest', (err, collection) ->
                    ids = ( doc._id for doc in docs )
                    collection.find( _id: $in: ids ).toArray (err, docsInDB) ->
                        docsInDB.length.should.equal 0
                        done()

        it 'updateOneOp should update only one document', (done) ->
            docs = ( {i: i} for i in [1,2,3] )
            transaction = committed.transaction "test", 'user', [
                {name: 'db.insert', arguments: ['instructionsTest', docs]}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                ids = ( d._id for d in docs )
                update = committed.transaction "test", 'user', [
                    {
                        name: 'db.updateOneOp'
                        arguments: ['instructionsTest', {_id: {__in: ids}, i: {__gte: 2}}, {__set: j: true}, {__unset: j: null} ]
                    }
                ]
                committed.enqueue update, (err,status) ->
                    should.not.exist err
                    status.should.be.string 'Committed'
                    _db.collection 'instructionsTest', (err, collection) ->
                        collection.find({j:true}).toArray (err, docsInDB) ->
                            docsInDB.length.should.equal 1
                            done()

        it 'updateOneOpRollback should undo an updateOneOp', (done) ->
            docs = ( {i: i} for i in [1,2,3] )
            transaction = committed.transaction "test", 'user', [
                {name: 'db.insert', arguments: ['instructionsTest', docs]}
            ]
            committed.enqueue transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                update = committed.transaction "test"
                ids = ( d._id for d in docs )
                update.instructions.push
                    name: 'db.updateOneOp'
                    arguments: ['instructionsTest', {_id: {__in: ids}, i: {__gte: 2}}, {__set: j: true}, {__unset: j: null} ]
                committed.enqueue update, (err,status) ->
                    should.not.exist err
                    status.should.be.string 'Committed'
                    rollback = committed.transaction "test", 'user', [
                        {
                            name: 'db.updateOneOpRollback'
                            arguments: ['instructionsTest', {_id: {__in: ids}, i: {__gte: 2}}, {__set: j: true}, {__unset: j: null} ]
                        }
                    ], [
                            name: 'db.pass'
                    ]
                    committed.enqueue rollback, (err,status) ->
                        should.not.exist err
                        status.should.be.string 'Committed'
                        _db.collection 'instructionsTest', (err, collection) ->
                            collection.find({j:true}).toArray (err, docsInDB) ->
                                docsInDB.length.should.equal 0
                                done()

        it 'revisionedUpdate should update a document with a correct revision', (done) ->
            masterDoc =
                revision: 
                    contentRevision: 1
                    otherRevision: 1
                content: 'here'
                otherContent: "other"
            oldDoc =
                revision: contentRevision: 1
                content: 'here'
            newDoc = 
                revision: contentRevision: 1
                content: 'here again'
            insert = committed.transaction "test"
            insert.instructions.push
                name: 'db.insert'
                arguments: ['instructionsTest', masterDoc]
            committed.enqueue insert, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                oldDoc._id = newDoc._id = masterDoc._id
                update = committed.transaction "test", 'user'
                update.instructions.push
                    name: 'db.revisionedUpdate'
                    arguments: ['instructionsTest', 'contentRevision', newDoc, oldDoc]
                committed.enqueue update, (err,status) ->
                    should.not.exist err
                    status.should.be.string 'Committed'
                    _db.collection 'instructionsTest', (err, collection) ->
                        collection.find({content:"here again"}).toArray (err, docsInDB) ->
                            docsInDB.length.should.equal 1
                            should.exist docsInDB[0]._id
                            delete docsInDB[0]._id
                            docsInDB[0].should.deep.equal 
                                revision: 
                                    contentRevision: 2
                                    otherRevision: 1
                                content: 'here again'
                                otherContent: "other"
                            done()

        it 'revisionedUpdateRollback should undo a revisionedUpdate', (done) ->
            masterDoc =
                revision: 
                    contentRevision: 1
                    otherRevision: 1
                content: 'here'
                otherContent: "other"
            oldDoc =
                revision: contentRevision: 1
                content: 'here'
            newDoc = 
                revision: contentRevision: 1
                content: 'here again'
            insert = committed.transaction "test"
            insert.instructions.push
                name: 'db.insert'
                arguments: ['instructionsTest', masterDoc]
            committed.enqueue insert, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                oldDoc._id = newDoc._id = masterDoc._id
                update = committed.transaction "test"
                update.instructions.push
                    name: 'db.revisionedUpdate'
                    arguments: ['instructionsTest', 'contentRevision', newDoc, oldDoc]
                committed.enqueue update, (err,status) ->
                    should.not.exist err
                    status.should.be.string 'Committed'
                    rollback = committed.transaction "test", 'user', [
                        {name: 'db.revisionedUpdateRollback', arguments: ['instructionsTest', 'contentRevision', newDoc, oldDoc]}
                    ], [
                        {name: 'db.pass'}
                    ]
                    committed.enqueue rollback, (err, status) ->
                        should.not.exist err
                        status.should.be.string 'Committed'
                        _db.collection 'instructionsTest', (err, collection) ->
                            collection.find({content:"here"}).toArray (err, docsInDB) ->
                                docsInDB.length.should.equal 1
                                should.exist docsInDB[0]._id
                                delete docsInDB[0]._id
                                docsInDB[0].should.deep.equal 
                                    revision: 
                                        contentRevision: 1
                                        otherRevision: 1
                                    content: 'here'
                                    otherContent: "other"
                                done()            

        it 'revisionedUpdate should not update a document whose revision has changed', (done) ->
            masterDoc =
                revision: 
                    contentRevision: 2
                    otherRevision: 1
                content: 'here'
                otherContent: "other"
            oldDoc =
                revision: contentRevision: 1
                content: 'here'
            newDoc = 
                revision: contentRevision: 1
                content: 'here'
                newContent: "a key which wont reach the db"
            insert = committed.transaction "test"
            insert.instructions.push
                name: 'db.insert'
                arguments: ['instructionsTest', masterDoc]
            committed.enqueue insert, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                oldDoc._id = newDoc._id = masterDoc._id
                update = committed.transaction "test", 'user', [], []
                update.instructions.push
                    name: 'db.revisionedUpdate'
                    arguments: ['instructionsTest', 'contentRevision', newDoc, oldDoc]
                update.rollback.push
                    name: 'db.revisionedUpdateRollback'
                    arguments: ['instructionsTest', 'contentRevision', newDoc, oldDoc]
                committed.enqueue update, (err,status) ->
                    should.not.exist err
                    status.should.be.string 'Failed'
                    _db.collection 'instructionsTest', (err, collection) ->
                        collection.find({content:"here"}).toArray (err, docsInDB) ->
                            docsInDB.length.should.equal 1
                            should.exist docsInDB[0]._id
                            delete docsInDB[0]._id
                            docsInDB[0].should.deep.equal 
                                revision: 
                                    contentRevision: 2
                                    otherRevision: 1
                                content: 'here'
                                otherContent: "other"
                            done()

    describe 'chained transactions', ->

        beforeEach (done) ->
            committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                should.not.exist err
                _db.createCollection 'chainedTest', (err, collection) -> 
                    collection.remove {}, {w:1}, done

        afterEach (done) ->
            committed.stop done

        it 'should execute all the instructions in each transaction in the chain', (done) ->
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q3', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                ]
            )
            committed.enqueue bigT, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                _db.collection 'chainedTest', {strict:true}, (err, collection) ->
                    should.not.exist err
                    collection.find({execute: 'all'}).toArray (err, docs) ->
                        docs.length.should.equal 6
                        done()


        it 'should execute their transactions in sequence, and within the sequence of other queues', (done) ->
            chainQueueCheck = (config, transaction, state, [what], done) ->
                _db.collection 'chainedTest', (err, collection) ->
                    should.exist what
                    collection.find({where: what}).toArray (err, docs) ->
                        docs.length.should.equal 1
                        done(null, true)
            committed.register 'chainQueueCheck', chainQueueCheck
            committed.register 'chainQueueCheckRollback', committed.db.pass

            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'chainQueueCheck', arguments: ['q1']}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'chainQueueCheck', arguments: ['q2']}
                    ]
                , 
                    committed.transaction 'q3', 'user', [
                        {name: 'chainQueueCheck', arguments: ['q3']}
                    ]
                ]
            )
            q1 = committed.transaction 'q1', 'user', [
                {name: "blockingMethod"}
                {name: 'db.insert', arguments: ['chainedTest', {where:'q1'}]}
            ]
            q2 = committed.transaction 'q2', 'user', [
                {name: "blockingMethod"}
                {name: 'db.insert', arguments: ['chainedTest', {where:'q2'}]}
            ]
            q3 = committed.transaction 'q3', 'user', [
                {name: "blockingMethod"}
                {name: 'db.insert', arguments: ['chainedTest', {where:'q3'}]}
            ]
            committed.enqueue q1, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue q2, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue q3, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.enqueue bigT, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
                done()

        it 'should return Failed if the first transaction in the chain fails, and roll it back', (done) ->
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'failMethod'}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q3', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                ]
            )
            committed.enqueue bigT, (err, status) ->
                should.not.exist err
                status.should.equal 'Failed'
                _db.collection 'chainedTest', (err, collection) ->
                    collection.find({execute: 'all'}).toArray (err, docs) ->
                        docs.length.should.equal 0
                        done()


        it 'should return ChainFailed if a non-first transaction fails, and rollback only that failed transaction', (done) ->
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'failMethod'}
                    ]
                , 
                    committed.transaction 'q3', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                ]
            )
            committed.enqueue bigT, (err, status) ->
                should.not.exist err
                status.should.equal 'ChainFailed'
                _db.collection 'chainedTest', (err, collection) ->
                    collection.find({execute: 'all'}).toArray (err, docs) ->
                        docs.length.should.equal 2
                        done()


        it 'should finish executing a chain before allowing exports.stop to callback', (done) ->
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                , 
                    committed.transaction 'q3', 'user', [
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                        {name: 'db.insert', arguments: ['chainedTest', {execute:'all'}]}
                    ]
                ]
            )
            committed.enqueue bigT, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'
            committed.stop (err) ->
                should.not.exist err
                _db.collection 'chainedTest', (err, collection) ->
                    collection.find({execute: 'all'}).toArray (err, docs) ->
                        docs.length.should.equal 6
                        #annoying afterEach does a stop, so start again
                        committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                            should.not.exist err
                            done()

    describe 'restart', ->
        
        beforeEach (done) ->
            _db.createCollection 'transactions', (err, collection) ->
                collection.remove {}, {w:1}, (err) ->
                    _db.createCollection 'restartTest', (err, collection) -> 
                        collection.remove {}, {w:1}, done

        afterEach (done) ->
            committed.stop done

        it 'should execute any transactions left at Queued status', (done) ->
            tinyT = committed.transaction null, 'user', [
                {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
            ]
            littleT = committed.transaction 'q1', 'user', [
                {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
            ]
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                    ]
                ]
            )
            #set positions, the first is an immediate 
            tinyT.position = -1
            littleT.position = 1
            bigT.position = 2
            #insert each directly into the db.
            async.each(
                [tinyT, littleT, bigT]
                , (item, itemDone) ->
                    _db.collection 'transactions', {strict:true}, (err, collection) ->
                        collection.insert item, {w:1, journal: true}, (err, objects) ->
                            should.not.exist err
                            itemDone()
                , (err) ->
                    should.not.exist err
                    committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                        should.not.exist err
                        _db.collection 'restartTest', {strict:true}, (err, collection) ->
                            should.not.exist err
                            collection.find({content:'here'}).toArray (err, docs) ->
                                docs.length.should.equal 7
                                done()
                )


        it 'should rollback any transaction left at Processing status', (done) ->
            #have to manually construct the rollback instructions because
            #_setUpTransaction isn't going to get called
            littleT = committed.transaction 'q1', 'user', [
                {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                {name: 'failMethod'}
            ], [
                {name: 'db.insertRollback', arguments: ['restartTest', {content:'here'}]}
                {name: 'db.pass'}
            ]
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'failMethod'}
                    ], [
                        {name: 'db.insertRollback', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.pass'}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                    ], [
                        {name: 'db.insertRollback', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.pass'}
                    ]
                ]
            )
            #set positions, the first is an immediate 
            littleT.position = 1
            bigT.position = 2
            doc1 = {_id: ObjectID(), content:'here'}
            doc2 = {_id: ObjectID(), content:'here'}
            #fake the state of this transaction to make it look like its half way through execution
            littleT.execution.state = [{ids:[doc1._id]}, {}]
            littleT.execution.result = [true, false]
            littleT.status = 'Processing'
            bigT.execution.state = [{ids:[doc2._id]}, {}]
            bigT.execution.result = [true, false]
            bigT.status = 'Processing'
            async.each(
                #put transactions and documents into the db
                [['transactions', [littleT, bigT]], ['restartTest', [doc1, doc2]]]
                , ([where, what], itemDone) ->
                    _db.collection where, {strict:true}, (err, collection) ->
                        collection.insert what, {w:1, journal: true}, (err, objects) ->
                            should.not.exist err
                            itemDone()
                , (err) ->
                    should.not.exist err
                    committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                        should.not.exist err
                        _db.collection 'restartTest', {strict:true}, (err, collection) ->
                            should.not.exist err
                            collection.find({content:'here'}).toArray (err, docs) ->
                                docs.length.should.equal 0
                                done()
                )


        it 'should restart execution of a stalled chain', (done) ->
            bigT = committed.chain(
                [
                    committed.transaction 'q1', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                    ]
                , 
                    committed.transaction 'q2', 'user', [
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                        {name: 'db.insert', arguments: ['restartTest', {content:'here'}]}
                    ]
                ]
            )
            #make the chain look like the first transaction has executed
            bigT.status = 'Committed'
            bigT.position = 1
            #insert it into the db and see if the second transaction in the chain is executed on startup.
            _db.collection 'transactions', {strict:true}, (err, collection) ->
                collection.insert bigT, {w:1, journal: true}, (err, objects) ->
                    should.not.exist err
                    committed.start {db:_db, softwareVersion:'testSoftwareVersion'}, (err) ->
                        should.not.exist err
                        _db.collection 'restartTest', {strict:true}, (err, collection) ->
                            should.not.exist err
                            collection.find({content:'here'}).toArray (err, docs) ->
                                docs.length.should.equal 2
                                done()
###


revision updateOneOp and insert; pass revision structure at start. increment all if none specified.

noDb flag?

make instructions into a class structure so that saving instructions state et al are easy for instruction authors to access

global lock needs to increment the revison of every document in the system.
pass revison info in at start, make insert / update aware of revision info.
this is a potentially slow update. need some kind of protection, but maybe not
this, eg if there was a models revision, this could be incremented to achieve
the same? 

###

    