// const dotenv = require("dotenv");
// const mysql = require("mysql2/promise");
// dotenv.config();

// // Create the connection pool
// const pool = mysql.createPool({
//   host: process.env.DB_HOST,
//   user: process.env.DB_USER,
//   password: process.env.DB_PASSWORD,
//   database: process.env.DB_DATABASE,
//   charset: 'utf8mb4',
//   waitForConnections: true,
//   connectionLimit: Number(process.env.connectionLimit) || 10,
//   queueLimit: 0
// });

// // Create a module with query and connection methods
// module.exports = {
//   // Generic query method
//   query: async (sql, params) => {
//     try {
//       const [results] = await pool.query(sql, params);
//       return [results];
//     } catch (error) {
//       console.error('Database Query Error:', error);
//       throw error;
//     }
//   },

//   // Method to get a connection from the pool
//   getConnection: async () => {
//     try {
//       const connection = await pool.getConnection();

//       // Enhance the connection object with transaction methods
//       connection.beginTransaction = async (isolationLevel = 'READ COMMITTED') => {
//         try {
//           await connection.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
//           await connection.query('START TRANSACTION');
//           return connection;
//         } catch (error) {
//           console.error('Begin Transaction Error:', error);
//           connection.release();
//           throw error;
//         }
//       };

//       return connection;
//     } catch (error) {
//       console.error('Get Connection Error:', error);
//       throw error;
//     }
//   },

//   // Utility method to end the pool
//   end: async () => {
//     await pool.end();
//   }
// };

/* CHATGPT DB SUGGESTION*/
const dotenv = require("dotenv");
const mysql = require("mysql2/promise");
dotenv.config();

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  charset: "utf8mb4",
  waitForConnections: true,
  connectionLimit: Number(process.env.connectionLimit) || 10,
  queueLimit: 0,
});

module.exports = {
  // simple one‑shot queries
  query: async (sql, params) => {
    const [rows] = await pool.query(sql, params);
    return [rows];
  },

  // get a pooled connection with “safe” tx helpers
  getConnection: async () => {
    const conn = await pool.getConnection();

    /* -------- enhanced transaction helpers -------- */

    // start tx + set isolation level
    conn.beginTransaction = async (level = "READ COMMITTED") => {
      await conn.query(`SET TRANSACTION ISOLATION LEVEL ${level}`);
      await conn.query("START TRANSACTION");
      conn.__inTx = true; // flag ON
    };

    // commit
    conn.commit = async () => {
      await conn.query("COMMIT");
      conn.__inTx = false; // flag OFF
    };

    // rollback only if we’re still inside a tx
    conn.rollback = async () => {
      if (conn.__inTx) {
        await conn.query("ROLLBACK");
        conn.__inTx = false;
      }
    };

    return conn;
  },

  // close pool when your app shuts down
  end: () => pool.end(),
};
