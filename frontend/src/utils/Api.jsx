import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://localhost:8000",
  // 👇 CRITICAL: This sends the HttpOnly cookie to the backend
  withCredentials: true,
  headers: {
    "Content-Type": "application/json",
  },
});

// --- NOTE: Request Interceptor Removed ---
// Since we switched to Cookies, we don't need to manually attach 
// the "Authorization: Bearer" header anymore. The browser does it for us.

// --- INTERCEPTOR: Handle 401 Errors (Session Expired) ---
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // If the backend says "401 Unauthorized", it means the Cookie is missing or expired
    if (error.response && error.response.status === 401) {
      console.warn("Unauthorized! Session expired or invalid cookie.");
      
      // Optional: Clear local storage just in case you store other user info
      localStorage.removeItem("user_data"); 
      
      // Redirect to login only if we aren't already there
      if (!window.location.pathname.includes('/auth/sign-in')) {
          window.location.href = "/auth/sign-in";
      }
    }
    return Promise.reject(error);
  }
);

export default api;