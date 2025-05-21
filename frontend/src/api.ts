import axios from 'axios';

// Get the API URL from environment variable or use a default
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// Create a regular API instance without authentication
export const api = axios.create({
  baseURL: API_URL,
}); 