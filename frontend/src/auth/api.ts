import axios from 'axios';
import { useAuth } from '@clerk/clerk-react';

// Get the API URL from environment variable or use a default
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const createAuthenticatedApi = () => {
  const { getToken } = useAuth();
  
  const api = axios.create({
    baseURL: API_URL,
  });
  
  // Add an interceptor to include the auth token in requests
  api.interceptors.request.use(async (config) => {
    const token = await getToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  });
  
  return api;
};

// Hook to use the authenticated API
export const useAuthenticatedApi = () => {
  return createAuthenticatedApi();
}; 