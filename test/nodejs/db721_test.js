var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`db721 extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"});
        conn = new duckdb.Connection(db);
        conn.exec(`LOAD '${process.env.QUACK_EXTENSION_BINARY_PATH}';`, function (err) {
            if (err) throw err;
            done();
        });
    });

    it('db721 function should return expected string', function (done) {
        db.all("SELECT db721('Sam') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: "Db721 Sam 🐥"}]);
            done();
        });
    });

    it('db721_openssl_version function should return expected string', function (done) {
        db.all("SELECT db721_openssl_version('Michael') as value;", function (err, res) {
            if (err) throw err;
            assert(res[0].value.startsWith('Db721 Michael, my linked OpenSSL version is OpenSSL'));
            done();
        });
    });
});