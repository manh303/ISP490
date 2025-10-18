import React, { useState } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router";
import Home from "./pages/Home";
import PublicLayout from "./layout/PublicLayout";
import SignIn from "./pages/AuthPages/SignIn";
import SignUp from "./pages/AuthPages/SignUp";
import ForgotPassword from "./pages/AuthPages/ForgotPassword";
import ResetPassword from "./pages/AuthPages/ResetPassword";
import VerifyCode from "./pages/AuthPages/VerifyCode";
import NotFound from "./pages/OtherPage/NotFound";
import UserProfiles from "./pages/UserProfiles";
import Videos from "./pages/UiElements/Videos";
import Images from "./pages/UiElements/Images";
import Alerts from "./pages/UiElements/Alerts";
import Badges from "./pages/UiElements/Badges";
import Avatars from "./pages/UiElements/Avatars";
import Buttons from "./pages/UiElements/Buttons";
import LineChart from "./pages/Charts/LineChart";
import BarChart from "./pages/Charts/BarChart";
import Calendar from "./pages/Calendar";
import BasicTables from "./pages/Tables/BasicTables";
import FormElements from "./pages/Forms/FormElements";
import Blank from "./pages/Blank";
import DashboardLayout from "./layout/DashboardLayout";
import VietnamElectronicsDashboard from "./pages/Dashboard/VietnamElectronicsDashboard";
import { ScrollToTop } from "./components/common/ScrollToTop";
// import Home from "./pages/Dashboard/Home";
import PasswordResetSuccess from "./components/auth/PasswordResetSuccess";
import { AuthProvider, ProtectedRoute } from "./contexts/AuthContext";
import { ToastProvider } from "./contexts/ToastContext";
import DSSPage from "./pages/DSSPage.jsx";
import AppLayout from "./layout/DashboardLayout";
import { AboutPage } from "./pages/Publics/AboutPage.js";
import { SolutionsPage } from "./pages/Publics/SolutionsPage.js";
import { ContactPage } from "./pages/Publics/ContactPage.js";
import { HomePage } from "./pages/Publics/HomePage.js";
import { LoginPage } from "./pages/Publics/LoginPage.js";
import { RegisterPage } from "./pages/Publics/RegisterPage.js";
import { ForgotPasswordPage } from "./pages/Publics/ForgotPasswordPage.js";
import { SendMessagePage } from "./pages/Publics/SendMessagePage.js";
import { ExplorePage } from "./pages/Publics/ExplorePage.js";
export type Page = "home" | "login" | "register" | "forgot-password" | "change-password" | "dashboard" | "scenario" | "revenue" | "forecast" | "operation" | "about" | "solutions" | "contact" | "send-message" | "explore";
export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>("home");
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  const handleLogin = () => {
    setIsLoggedIn(true);
    setCurrentPage("dashboard");
  };

  const handleLogout = () => {
    setIsLoggedIn(false);
    setCurrentPage("home");
  };

  const navigateTo = (page: Page) => {
    // Public pages - anyone can access
    if (page === "login" || page === "register" || page === "forgot-password" || page === "home" || page === "about" || page === "solutions" || page === "contact" || page === "send-message" || page === "explore") {
      setCurrentPage(page);
    }
    // Protected pages - need login
    else if (isLoggedIn) {
      setCurrentPage(page);
    } else {
      setCurrentPage("login");
    }
  };
  return (
    <AuthProvider>
      <ToastProvider>
        <Router>
          <ScrollToTop />
          <Routes>
            {/* Dashboard Layout */}
            <Route element={<DashboardLayout />}>
              <Route path="/dashboard" element={<VietnamElectronicsDashboard />} />
            </Route>
            {/* Public Auth Routes (defined below as simple routes) */}

            {/* Protected Dashboard Layout */}
            <Route
              element={
                <ProtectedRoute>
                  <AppLayout />
                </ProtectedRoute>
              }
            >
              {/* Dashboard routes - all protected */}
              <Route path="/dashboard" element={<VietnamElectronicsDashboard />} />
              <Route path="/dss" element={<DSSPage />} />
              <Route path="/profile" element={<UserProfiles />} />
              <Route path="/calendar" element={<Calendar />} />
              <Route path="/blank" element={<Blank />} />

              {/* Forms */}
              <Route path="/form-elements" element={<FormElements />} />

              {/* Tables */}
              <Route path="/basic-tables" element={<BasicTables />} />

              {/* Ui Elements */}
              <Route path="/alerts" element={<Alerts />} />
              <Route path="/avatars" element={<Avatars />} />
              <Route path="/badge" element={<Badges />} />
              <Route path="/buttons" element={<Buttons />} />
              <Route path="/images" element={<Images />} />
              <Route path="/videos" element={<Videos />} />

              {/* Charts */}
              <Route path="/line-chart" element={<LineChart />} />
              <Route path="/bar-chart" element={<BarChart />} />
            </Route>
            {/* Public Layout */}
            <Route element={<PublicLayout />}>
              <Route path="/home" element={<Home />} />
              <Route
                path="/public/home"
                element={<HomePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />}
              />
              <Route path="/public/login" element={<LoginPage onLogin={handleLogin} navigateTo={navigateTo} />} />
              <Route path="/public/register" element={<RegisterPage navigateTo={navigateTo} />} />
              <Route path="/public/forgot-password" element={<ForgotPasswordPage navigateTo={navigateTo} />} />
              <Route path="/about" element={<AboutPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
              <Route path="/solutions" element={<SolutionsPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
              <Route path="/contact" element={<ContactPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
              <Route path="/send-message" element={<SendMessagePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
              <Route path="/explore" element={<ExplorePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            </Route>

            {/* Auth Layout */}
            <Route path="/signin" element={<SignIn />} />
            <Route path="/signup" element={<SignUp />} />
            <Route path="/forgot-password" element={<ForgotPassword />} />
            <Route path="/reset-password/:token" element={<ResetPassword />} />
            <Route path="/verify-code" element={<VerifyCode />} />
            <Route path="/password-reset-success" element={<PasswordResetSuccess />} />

            {/* Fallback Route */}
            <Route path="*" element={<NotFound />} />
          </Routes>
        </Router>
      </ToastProvider>
    </AuthProvider>
  );
}
