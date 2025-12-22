import axios from "axios";
// import dotenv from "dotenv"

// dotenv.config();

const api = axios.create({
  // baseURL: process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:5000", 
  baseURL: import.meta.env.VITE_API_URL || "http://127.0.0.1:5000", 
  headers: {
    "Content-Type": "application/json",
  },
});

export default api;
