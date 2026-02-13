import { Routes, Route, Navigate } from "react-router-dom";
import Dashboard from "./layouts/dashboard";
import PrivateRoute from "./auth/PrivateRoute";
import { SignIn } from "./pages/auth/sign-in";
// import { SignUp } from "./pages/auth/sign-up";  <-- 1. Remove or Comment this import
import { AuthProvider } from "@/context/AuthContext";

function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route path="/dashboard/*" element={
          <PrivateRoute>
             <Dashboard />
          </PrivateRoute>
        } />

        {/* 2. Keep only Sign In */}
        <Route path="/auth/sign-in" element={<SignIn />} />

        {/* 3. Remove the Sign Up route so it cannot be accessed */}
        {/* <Route path="/auth/sign-up" element={<SignUp />} /> */}

        {/* 4. CHANGE REDIRECT: Send unknown paths to sign-in instead of sign-up */}
        <Route path="*" element={<Navigate to="/auth/sign-in" replace />} />
      </Routes>
    </AuthProvider>
  );
}

export default App;