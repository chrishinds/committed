async = require 'async'
chai = require 'chai'
mongodb = require 'mongodb'
uuid = require 'node-uuid'

should = chai.should()

committed = require './../committed'

_db = null
dbconfig =
    server   : "127.0.0.1"
    port     : "27017"
    database : "committedtest"

before (done) ->
    # Connect to the database
    dbaddress = 'mongodb://' + dbconfig.server + '/' + dbconfig.database

    mongodb.MongoClient.connect dbaddress, (err, db) ->
        if err? then done(err)
        else
            _db = db
            done()

describe 'Committed', ->

    describe 'startup', ->

        it 'should start without error', (done) ->
            committed.start _db, 'testSoftwareVersion', (err) ->
                done err

        it 'should not start a second time', (done) ->
            committed.start _db, 'testSoftwareVersion', (err) ->
                err.message.should.have.string 'committed has already been started'
                done()

        it 'should stop without error only if started', (done) ->
            committed.stop (err) ->
                should.not.exist err
                committed.stop (err) ->
                    err.message.should.have.string 'committed is not currently started'
                    done()

        it 'should accept the predefined named functions (once)', (done) ->
            committed.register 'db', committed.db
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
            committed.start _db, 'testSoftwareVersion', (err) ->
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
                committed.sequentially transaction, next

            start = new Date().getTime()

            async.times 100, task, (err) ->
                transactionTime = new Date().getTime() - start
                should.not.exist err

                task = (n, next) ->
                    _db.collection('validOpsTest').insert {value: n + 20}, {w:1, journal: true, upsert:false, multi: false, serializeFunctions: false}, next

                start = new Date().getTime()
                async.timesSeries 100, task, (err) ->
                    should.not.exist err
                    directTime = new Date().getTime() - start

                    (transactionTime / directTime).should.be.below 4
                    done()

        it 'should be possible to place transactions in multiple queues', (done) ->
            # Push 10 transactions into each of 10 queues

            tasks = []
            for t in [1..10]
                do (t) ->
                    for q in [1..10]
                        do (q) ->
                            tasks.push (done) ->
                                transaction = committed.transaction "testQ#{q}"
                                transaction.instructions.push
                                    name: 'db.insert'
                                    arguments: ['validOpsTest', {t:t, q:q}]
                                committed.sequentially transaction, (err, status) ->
                                    should.not.exist err
                                    status.should.be.string 'Committed'
                                    done()

            async.parallel tasks, done

        it 'should unpack and execute a transaction with a constructor', (done) ->
            constructor = (data, done) ->
                [instructions, rollback] = [[],[]]
                instructions.push
                    name: 'db.insert'
                    arguments: [data.target, {value: data.value}]
                done null, instructions, rollback
            transaction = committed.transaction 'testQ'
            transaction.constructor = constructor
            id = uuid.v4()
            transaction.data =
                target: 'validOpsTest'
                value: id
            committed.sequentially transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Committed'
                _db.collection('validOpsTest').count {value: id}, (err, count) ->
                    should.not.exist err
                    count.should.equal 1
                    done()


    describe 'global lock', ->

        before (done) ->
            blockingMethod = (db, transactionData, finishObj, done) ->
                untilTest = () -> finishObj.finished
                untilFn = (done) ->
                    # just sleep for a bit
                    setTimeout done, 100
                async.until untilTest, untilFn, () ->
                    done()
            committed.register 'blockingMethod', blockingMethod

            committed.start _db, 'testSoftwareVersion', (err) ->
                should.not.exist err
                _db.createCollection 'globalLockTest', done

        after (done) ->
            committed.stop done

        it 'should close and reopen normal queues when processing a GlobalLock transaction', (done) ->
            finishObj = { finished: false }

            globalTransaction = committed.transaction 'GlobalLock'
            globalTransaction.instructions.push
                name: 'blockingMethod'
                arguments: [finishObj]
            committed.withGlobalLock globalTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'

                # make sure the queues are open again

                nonGlobalTransaction = committed.transaction 'nonGlobal'
                committed.sequentially nonGlobalTransaction, (err, status) ->
                    status.should.equal 'Committed'
                    done(err)

            nonGlobalTransaction = committed.transaction 'nonGlobal'
            committed.sequentially nonGlobalTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Failed'
                finishObj.finished = true

        it 'should suspend and resume immediate processing when processing a GlobalLock transaction', (done) ->
            finishObj = { finished: false }

            globalTransaction = committed.transaction 'GlobalLock'
            globalTransaction.instructions.push
                name: 'blockingMethod'
                arguments: [finishObj]
            committed.withGlobalLock globalTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Committed'

                immediateTransaction = committed.transaction()
                committed.immediately immediateTransaction, (err, status) ->
                    status.should.equal 'Committed'
                    done(err)

            immediateTransaction = committed.transaction()
            committed.immediately immediateTransaction, (err, status) ->
                should.not.exist err
                status.should.equal 'Failed'
                finishObj.finished = true

    describe 'rollback', ->

        before (done) ->
            failMethod = (db, transactionData, done) ->
                done(null, false)
            committed.register 'failMethod', failMethod

            errorMethod = (db, transactionData, done) ->
                done 'Supposed to fail', false
            committed.register 'errorMethod', errorMethod

            committed.start _db, 'testSoftwareVersion', (err) ->
                should.not.exist err
                _db.createCollection 'rollbackTest', done

        after (done) ->
            committed.stop done

        it 'should rollback an errored transaction and return status FailedCommitErrorRollbackOk', (done) ->
            insertId = uuid.v4()

            transaction = committed.transaction 'rollbackTest'
            transaction.instructions.push
                name: 'db.insert'
                arguments: ['rollbackTest', {id: insertId, rolledback: false}]
            transaction.instructions.push
                name: 'errorMethod'
            transaction.rollback.push
                name: 'db.updateOneField'
                arguments: ['rollbackTest', {id: insertId}, 'rolledback', true]

            committed.sequentially transaction, (err, status) ->
                status.should.equal 'FailedCommitErrorRollbackOk'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count {id: insertId, rolledback: true}, (err, count) ->
                    count.should.equal 1
                    done(err)

        it 'should rollback a failed transaction and return status Failed', (done) ->
            insertId = uuid.v4()

            transaction = committed.transaction 'rollbackTest'
            transaction.instructions.push
                name: 'db.insert'
                arguments: ['rollbackTest', {id: insertId, rolledback: false}]
            transaction.instructions.push
                name: 'failMethod'
            transaction.rollback.push
                name: 'db.updateOneField'
                arguments: ['rollbackTest', {id: insertId}, 'rolledback', true]

            committed.sequentially transaction, (err, status) ->
                status.should.equal 'Failed'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count {id: insertId, rolledback: true}, (err, count) ->
                    count.should.equal 1
                    done(err)

        it 'should report a catastrophe if a rollback fails after an error', (done) ->
            insertId = uuid.v4()

            transaction = committed.transaction 'rollbackTest'
            transaction.instructions.push
                name: 'db.insert'
                arguments: ['rollbackTest', {id: insertId, rolledback: false}]
            transaction.instructions.push
                name: 'errorMethod'
            transaction.rollback.push
                name: 'db.updateOneField'
                arguments: ['rollbackTest', {id: insertId}, 'rolledback', true]
            transaction.rollback.push
                name: 'errorMethod'

            committed.sequentially transaction, (err, status) ->
                status.should.equal 'CatastropheCommitErrorRollbackError'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count {id: insertId, rolledback: true}, (err, count) ->
                    count.should.equal 1
                    done(err)

        it 'should report a catastrophe if a rollback fails after a failure', (done) ->
            insertId = uuid.v4()

            transaction = committed.transaction 'rollbackTest'
            transaction.instructions.push
                name: 'db.insert'
                arguments: ['rollbackTest', {id: insertId, rolledback: false}]
            transaction.instructions.push
                name: 'failMethod'
            transaction.rollback.push
                name: 'db.updateOneField'
                arguments: ['rollbackTest', {id: insertId}, 'rolledback', true]
            transaction.rollback.push
                name: 'failMethod'

            committed.sequentially transaction, (err, status) ->
                status.should.equal 'CatastropheCommitFailedRollbackError'
                # 'manually' check that we rolled back
                _db.collection('rollbackTest').count {id: insertId, rolledback: true}, (err, count) ->
                    count.should.equal 1
                    done(err)

        it 'should run a constructed rollback if there is a constructor', (done) ->
            constructor = (data, done) ->
                [instructions, rollback] = [[],[]]
                instructions.push
                    name: 'db.insert'
                    arguments: [data.target, {value: data.value}]
                instructions.push
                    name: 'failMethod'
                rollback.push
                    name: 'db.updateOneField'
                    arguments: [data.target, {value: data.value}, 'rolledback', true]
                done null, instructions, rollback
            transaction = committed.transaction 'testQ'
            transaction.constructor = constructor
            id = uuid.v4()
            transaction.data =
                target: 'rollbackTest'
                value: id
            committed.sequentially transaction, (err, status) ->
                should.not.exist err
                status.should.be.string 'Failed'
                _db.collection('rollbackTest').count {value: id, rolledback: true}, (err, count) ->
                    should.not.exist err
                    count.should.equal 1
                    done()

    describe 'error handling', ->

        before (done) ->
            committed.start _db, 'testSoftwareVersion', done

        after (done) ->
            committed.stop done

        it 'should reject a transaction for processing with an invalid queue name', (done) ->

            tests = []

            tests.push (done) ->
                globalTransaction = committed.transaction 'GlobalLock'
                committed.sequentially globalTransaction, (err) ->
                    err.message.should.have.string "Can't queue a transaction for GlobalLock using the committed.sequentially function"
                    done()

            tests.push (done) ->
                nullQueueTransaction = committed.transaction null
                committed.sequentially nullQueueTransaction, (err) ->
                    err.message.should.have.string 'must have a transaction.queue parameter to use committed.sequentially'
                    done()

            tests.push (done) ->
                emptyQueueTransaction = committed.transaction ''
                committed.sequentially emptyQueueTransaction, (err) ->
                    err.message.should.have.string 'must have a transaction.queue parameter to use committed.sequentially'
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
                    committed.sequentially transaction, (err, status) ->
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
                    committed.start _db, 'testSoftwareVersion', done