const express = require('express')
const app = express()
const port = 8383

app.use(express.static('public'))

app.get('/api/query', async (req, res) => {
    try {
        await connect();
        const result = await q1();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})


app.get('/api/query2', async (req, res) => {
    try {
        await connect();
        const result = await q2();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})

app.get('/api/query3', async (req, res) => {
    try {
        await connect();
        const result = await q3();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})

app.get('/api/query4', async (req, res) => {
    try {
        await connect();
        const result = await q4();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})

app.get('/api/query5', async (req, res) => {
    try {
        await connect();
        const result = await q5();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})

app.get('/api/query6', async (req, res) => {
    try {
        await connect();
        const result = await q6();
        res.json(result);
    } catch (error) {
        console.error(`Error in /api/query endpoint: ${error}`);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        await client.close();
    }

})

app.listen(port, () => console.log(`server has started on ${port}`))

const {MongoClient} = require("mongodb")

const uri = "mongodb://localhost:27017"

const client = new MongoClient(uri);
const db = client.db("myBookStore");

connect();
async function connect() {


    try {
        await client.connect();

        console.log(`Connected to database ${db.databaseName}`)

    }
    catch (ex){
        console.error(`Something went wrong lol ${ex}`)
    }
    finally {
    }
}

async function q1() {

const books = db.collection("books");

const pipe = [
    {
        '$sort': {
            'cost': -1
        }
    }
];

const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
test = [];
for await (const doc of aggCursor) {
    //console.log(doc);
    test.push(doc)
}
    return test;
}

async function q2() {

    const books = db.collection("books");

    const pipe = [
            {
                '$group': {
                    '_id': '$genre',
                    'count': {
                        '$count': {}
                    }
                }
            }
    ];

    const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
    test = [];
    for await (const doc of aggCursor) {
        //console.log(doc);
        test.push(doc)
    }
    return test;
}

// QUERY 3
async function q3() {

    const books = db.collection("books");

    const pipe = [
            {
                '$group': {
                    '_id': null,
                    'avgCost': {
                        '$avg': '$cost'
                    }
                }
            }, {
            '$lookup': {
                'from': 'books',
                'localField': '_id',
                'foreignField': 'id',
                'as': 'books'
            }
        }, {
            '$unwind': {
                'path': '$books'
            }
        }, {
            '$project': {
                '_id': '$books._id',
                'title': '$books.title',
                'cost': '$books.cost',
                'avg': '$avgCost',
                'genre': '$books.genre',
                'publisher': '$books.publisher',
                'year': '$books.year',
                'ISBN': '$books.ISBN'
            }
        }, {
            '$match': {
                'genre': 'poetry',
                '$expr': {
                    '$gt': [
                        '$cost', '$avg'
                    ]
                }
            }
        }, {
            '$project': {
                '_id': '$_id',
                'title': '$title',
                'cost': '$cost',
                'genre': '$genre',
                'publisher': '$publisher',
                'year': '$year',
                'ISBN': '$ISBN'
            }
        }
    ];

    const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
    test = [];
    for await (const doc of aggCursor) {
        //console.log(doc);
        test.push(doc)
    }
    return test;
}

// QUERY 4
async function q4() {

    const books = db.collection("books");

    const pipe = [
            {
                '$lookup': {
                    'from': 'authors',
                    'localField': 'authorID',
                    'foreignField': '_id',
                    'as': 'authors'
                }
            }, {
            '$unwind': {
                'path': '$authors'
            }
        }, {
            '$project': {
                '_id': '$_id',
                'title': '$title',
                'genre': '$genre',
                'publisher': '$publisher',
                'year': '$year',
                'ISBN': '$ISBN',
                'cost': '$cost',
                'fname': '$authors.firstname',
                'lname': '$authors.lastname',
                'origin': '$authors.country'
            }
        }, {
            '$group': {
                '_id': {
                    '$concat': [
                        '$fname', ' ', '$lname'
                    ]
                },
                'total_books': {
                    '$count': {}
                }
            }
        }, {
            '$sort': {
                'total_books': -1
            }
        }
    ];

    const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
    test = [];
    for await (const doc of aggCursor) {
        //console.log(doc);
        test.push(doc)
    }
    return test;
}

// QUERY 5
async function q5() {

    const books = db.collection("books");

    const pipe = [
            {
                '$group': {
                    '_id': {
                        'author': '$authorID',
                        'publisher': '$publisher'
                    }
                }
            }, {
            '$project': {
                'author': '$_id.author',
                'publisher': '$_id.publisher'
            }
        }, {
            '$group': {
                '_id': '$publisher',
                'count': {
                    '$count': {}
                }
            }
        }
    ];

    const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
    test = [];
    for await (const doc of aggCursor) {
        //console.log(doc);
        test.push(doc)
    }
    return test;
}

// QUERY 6

async function q6() {

    const books = db.collection("books");

    const pipe = [
            {
                '$lookup': {
                    'from': 'authors',
                    'localField': 'authorID',
                    'foreignField': '_id',
                    'as': 'authors'
                }
            }, {
                '$unwind': {
                    'path': '$authors'
                }
            }, {
                '$project': {
                    '_id': '$_id',
                    'title': '$title',
                    'genre': '$genre',
                    'publisher': '$publisher',
                    'year': '$year',
                    'ISBN': '$ISBN',
                    'cost': '$cost',
                    'fname': '$authors.firstname',
                    'lname': '$authors.lastname',
                    'origin': '$authors.country'
                }
            }, {
                '$group': {
                    '_id': {
                        '$concat': [
                            '$fname', ' ', '$lname'
                        ]
                    },
                    'total_cost': {
                        '$sum': '$cost'
                    }
                }
            }, {
                '$sort': {
                    'total_cost': -1
                }
            }
    ];

    const aggCursor= books.aggregate(pipe);
//console.log(aggCursor)
    test = [];
    for await (const doc of aggCursor) {
        //console.log(doc);
        test.push(doc)
    }
    return test;
}

//console.log(q1())

