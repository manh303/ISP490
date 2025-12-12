import React, { useState } from "react";
// import DashboardLayout from "././layout/DashboardLayout";
import { Dashboard } from "./pages/Publics/Dashboard";
import { BrowserRouter as Router, Routes, Route, useNavigate } from "react-router-dom";
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
import DashboardLayout from "././layout/DashboardLayout";
import AdminLayout from "././layout/AdminLayout";
import AnalystLayout from "././layout/AnalystLayout";
import CustomerLayout from "././layout/CustomerLayout";
import DataEngineerLayout from "././layout/DataEngineerLayout";
import MLILayout from "././layout/MLILayout";
import VietnamElectronicsDashboard from "./pages/Dashboard/VietnamElectronicsDashboard";
import RoleBasedDashboard from "./components/dashboard/RoleBasedDashboard";
import AppLayout from "././layout/AppLayout";
import { ScrollToTop } from "./components/common/ScrollToTop";
// import Home from "./pages/Dashboard/Home";
import PasswordResetSuccess from "./components/auth/PasswordResetSuccess";
import { AuthProvider, ProtectedRoute } from "./contexts/AuthContext";
import { ToastProvider } from "./contexts/ToastContext";
import DSSPage from "./pages/DSSPage.jsx";
import { AboutPage } from "./pages/Publics/AboutPage.js";
import { SolutionsPage } from "./pages/Publics/SolutionsPage.js";
import { ContactPage } from "./pages/Publics/ContactPage.js";
import { HomePage } from "./pages/Publics/HomePage.js";
import { SendMessagePage } from "./pages/Publics/SendMessagePage.js";
import { ExplorePage } from "./pages/Publics/ExplorePage.js";
import AdminPage from "./pages/Admin/AdminPage.js";
import AdminUserManagement from "./pages/Admin/AdminUserManagement";
import AdminDashboard from "./pages/Admin/AdminDashboard";
import RoleManagement from "./pages/Admin/RoleManagement";
import ActivityLogsPage from "./pages/Admin/ActivityLogsPage";
import ActivityStatsPage from "./pages/Admin/ActivityStatsPage";
import AnalystPage from "./pages/Analyst/AnalystPage.js";
import CustomerPage from "./pages/Customer/CustomerPage.js";
// import { Dashboard } from "./pages/Publics/Dashboard.js";
import UserDetails from "./components/admin/UserDetails";
import { useParams } from "react-router-dom";
import DeletedUsersList from "./components/admin/DeletedUsersList.js";
import { AnalystWireframe } from "./pages/Analyst/AnalystWireframe.js";
import { DataEngineerWireframe } from "./pages/DataEngineer/DataEngineerWireframe.js";
import MLIPage from "./pages/MLInsights/MLIPage.js";
import { AnalyticsDashboard } from "./pages/Analyst/AnalyticsDashboard.js";
import { ProductAnalytics } from "./pages/Analyst/ProductAnalytics.js";
import { ReviewAnalytics } from "./pages/Analyst/ReviewAnalytics.js";
import { PlatformAnalytics } from "./pages/Analyst/PlatformAnalytics.js";
import { CategoryAnalytics } from "./pages/Analyst/CategoryAnalytics.js";
import { PricingAnalytics } from "./pages/Analyst/PricingAnalytics.js";
import { ProductDetailAnalytics } from "./pages/Analyst/ProductDetailAnalytics.js";
import ModelDashboard from "./pages/Analyst/ModelDashboard";
import DSSInput from "./pages/Analyst/DSSInput";
import ProductReviewDetails from "./pages/Analyst/ProductReviewDetails";
import MLOverview from "./pages/MLInsights/MLOverview.js";
import PriceIntelligence from "./pages/MLInsights/PriceIntelligence.js";
import DemandSalesForecasting from "./pages/MLInsights/DemandSalesForecasting.js";
import ProductMLInsights from "./pages/MLInsights/ProductMLInsights.js";
import {
  ModelsListPage,
  ModelDetailPage,
  CreateModelPage,
  PricePredictionPage,
  RecommendationsPage,
  SentimentAnalysisPage,
  StatusOverviewPage
} from "./pages/MachineLearning";
import DataEngineerDashboard from "./pages/DataEngineer/DataEngineerDashboard.tsx";
import DataPipeline from "./pages/DataEngineer/DataPipeline.tsx";
import DataQuality from "./pages/DataEngineer/DataQuality.tsx";
import TableGrowthPage from "./pages/DataEngineer/TableGrowthPage.tsx";
import DataQualitySummaryPage from "./pages/DataEngineer/DataQualitySummaryPage.tsx";
import TableLineagePage from "./pages/DataEngineer/TableLineagePage.tsx";
import AlertHistoryPage from "./pages/DataEngineer/AlertHistoryPage.tsx";
import PipelinePerformancePage from "./pages/DataEngineer/PipelinePerformancePage.tsx";
import DataVolumeTrendsPage from "./pages/DataEngineer/DataVolumeTrendsPage.tsx";
import DSSResults from "./pages/Analyst/DSSResults.tsx";
import ReportsPage from "./pages/Analyst/ReportsPage";
import DSSDecisionsPage from "./pages/Analyst/DSSDecisionsPage";
import DSSDecisionDetailPage from "./pages/Analyst/DSSDecisionDetailPage";
import DSSDecisionCreatePage from "./pages/Analyst/DSSDecisionCreatePage";
import DSSSessionsPage from "./pages/Analyst/DSSSessionsPage";

// Business Metadata Pages
import SourcesPage from "./pages/DataEngineer/SourcesPage";
import CatalogPage from "./pages/DataEngineer/CatalogPage";
import GlossaryPage from "./pages/DataEngineer/GlossaryPage";
import ExpectationsJobsPage from "./pages/DataEngineer/ExpectationsJobsPage";
import DSSScenarios from "./pages/Analyst/DSSScenarios.tsx";
// import AdminDashboard from "./components/dashboard/roles/AdminDashboard.tsx";

// Remove DashboardLayoutWrapper, use DashboardLayout as a layout route
function UserDetailsWrapper() {
  const { userId } = useParams();
  const id = userId ? Number(userId) : null;
  if (id === null || isNaN(id)) return <div>Invalid user ID</div>;
  return <UserDetails userId={id} onBack={() => window.history.back()} />;
}
export type Page = "home" | "login" | "register" | "forgot-password" | "change-password" | "dashboard" | "scenario" | "revenue" | "forecast" | "operation" | "about" | "solutions" | "contact" | "send-message" | "explore";
export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>("home");
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  // Helper chuyển page sang path
  const pageToPath = (page: Page) => {
    switch (page) {
      case "home":
        return "/";
      case "login":
        return "/signin";
      case "register":
        return "/signup";
      case "forgot-password":
        return "/forgot-password";
      case "dashboard":
        return "/dashboard";
      case "about":
        return "/about";
      case "solutions":
        return "/solutions";
      case "contact":
        return "/contact";
      case "send-message":
        return "/send-message";
      case "explore":
        return "/explore";
      default:
        return "/";
    }
  };

  // // Hàm login/logout giữ nguyên
  // const handleLogin = () => {
  //   setIsLoggedIn(true);
  //   setCurrentPage("home");
  // };
  const handleLogout = () => {
    setIsLoggedIn(false);
    setCurrentPage("home");
  };

  // Component con nằm trong <Router>
  function AppContent() {
    const navigate = useNavigate();
    const navigateTo = (page: Page) => {
      // Public pages - anyone can access
      if (
        page === "login" ||
        page === "register" ||
        page === "forgot-password" ||
        page === "home" ||
        page === "about" ||
        page === "solutions" ||
        page === "contact" ||
        page === "send-message" ||
        page === "explore"
      ) {
        setCurrentPage(page);
        const path = pageToPath(page);
        navigate(path);
      }
      // Protected pages - need login
      else if (isLoggedIn) {
        setCurrentPage(page);
        const path = pageToPath(page);
        navigate(path);
      } else {
        setCurrentPage("login");
        const path = pageToPath("login");
        navigate(path);
      }
    };
    return (
      <>
        <ScrollToTop />
        <Routes>
          {/* Admin Layout - Protected Routes */}
          <Route
            element={
              <ProtectedRoute requiredRole="ADMIN">
                <AdminLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/admin/home" element={<AdminPage />} />
            <Route path="/admin/dashboard" element={<AdminDashboard />} />
            <Route path="/admin/users" element={<AdminUserManagement />} />
            <Route path="/admin/roles" element={<RoleManagement />} />
            {/* <Route path="/admin/users/details" element={<UserProfiles />} /> */}
            <Route path="/admin/deleted-users" element={<DeletedUsersList onSelectUser={() => { }} />} />
            <Route path="/admin/analytics" element={<DSSPage />} />
            <Route path="/admin/tables" element={<BasicTables />} />
            <Route path="/admin/export" element={<Blank />} />
            <Route path="/admin/import" element={<Blank />} />
            <Route path="/admin/settings/general" element={<Blank />} />
            <Route path="/admin/settings/security" element={<Blank />} />
            <Route path="/admin/settings/permissions" element={<Blank />} />
            <Route path="/admin/logs" element={<Blank />} />
            <Route path="/admin/activity-logs" element={<ActivityLogsPage />} />
            <Route path="/admin/activity-stats" element={<ActivityStatsPage />} />
            <Route path="/admin/performance" element={<Blank />} />
            <Route path="/admin/errors" element={<Blank />} />
            <Route path="/admin/notifications" element={<Blank />} />
            <Route path="/admin/profile" element={<UserProfiles />} />
          </Route>

          {/* Analyst Layout - Protected Routes */}
          <Route
            element={
              <ProtectedRoute requiredRole={["ANALYST", "ADMIN"]}>
                <AnalystLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/analyst/home" element={<AnalystPage />} />
            <Route path="/analyst/dashboard" element={<AnalystWireframe />} />
            <Route path="/analyst/model-dashboard" element={<ModelDashboard />} />
            <Route path="/analyst/dss/:modelId" element={<DSSInput />} />
            <Route path="/analyst/dss/:modelId/results" element={<DSSResults />} />
            <Route path="/analyst/dss-scenarios" element={<DSSScenarios />} />
            <Route path="/analyst/product-review/:productKey" element={<ProductReviewDetails />} />
            <Route path="/analyst/dss-decisions" element={<DSSDecisionsPage />} />
            <Route path="/analyst/dss-sessions" element={<DSSSessionsPage />} />
            <Route path="/analyst/dss-decisions/create" element={<DSSDecisionCreatePage />} />
            <Route path="/analyst/dss-decisions/:decisionId" element={<DSSDecisionDetailPage />} />
            <Route path="/analyst/analytics-dashboard" element={<AnalyticsDashboard />} />
            <Route path="/analyst/product-analytics" element={<ProductAnalytics />} />
            <Route path="/analyst/review-analytics" element={<ReviewAnalytics />} />
            <Route path="/analyst/platform-analytics" element={<PlatformAnalytics />} />
            <Route path="/analyst/category-analytics" element={<CategoryAnalytics />} />
            <Route path="/analyst/pricing-analytics" element={<PricingAnalytics />} />
            <Route path="/analyst/product-detail-analytics" element={<ProductDetailAnalytics />} />
            <Route path="/analyst/reports" element={<ReportsPage />} />
            <Route path="/analyst/sales" element={<BarChart />} />
            <Route path="/analyst/trends" element={<LineChart />} />
            <Route path="/analyst/customers" element={<DSSPage />} />
            <Route path="/analyst/products" element={<VietnamElectronicsDashboard />} />
            <Route path="/analyst/reports/weekly" element={<BasicTables />} />
            <Route path="/analyst/reports/monthly" element={<BasicTables />} />
            <Route path="/analyst/reports/custom" element={<BasicTables />} />
            <Route path="/analyst/charts" element={<BarChart />} />
            <Route path="/analyst/interactive" element={<VietnamElectronicsDashboard />} />
            <Route path="/analyst/explorer" element={<BasicTables />} />
            <Route path="/analyst/query-builder" element={<Blank />} />
            <Route path="/analyst/data-mining" element={<Blank />} />
            <Route path="/analyst/models" element={<Blank />} />
            <Route path="/analyst/schedule" element={<Calendar />} />
            <Route path="/analyst/refresh" element={<Blank />} />
            <Route path="/analyst/alerts" element={<Alerts />} />
            <Route path="/analyst/profile" element={<UserProfiles />} />
          </Route>
          {/* DataEngineerSidebar Layout - Protected Routes */}
          <Route
            element={
              <ProtectedRoute requiredRole="DATA_ENGINEER">
                <DataEngineerLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/dataengineer/dashboard" element={<DataEngineerDashboard />} />
            <Route path="/dataengineer/pipelines" element={<DataPipeline />} />
            <Route path="/dataengineer/pipelines/:jobCode" element={<DataPipeline />} />
            <Route path="/dataengineer/quality" element={<DataQuality />} />
            <Route path="/dataengineer/table-growth" element={<TableGrowthPage />} />
            <Route path="/dataengineer/data-quality-summary" element={<DataQualitySummaryPage />} />
            <Route path="/dataengineer/table-lineage" element={<TableLineagePage />} />
            <Route path="/dataengineer/alert-history" element={<AlertHistoryPage />} />
            <Route path="/dataengineer/pipeline-performance" element={<PipelinePerformancePage />} />
            <Route path="/dataengineer/data-volume-trends" element={<DataVolumeTrendsPage />} />
            <Route path="/dataengineer/jobs" element={<Blank />} />
            <Route path="/dataengineer/schedules" element={<Calendar />} />
            <Route path="/dataengineer/monitoring" element={<Alerts />} />
            <Route path="/dataengineer/logs" element={<Blank />} />
            <Route path="/dataengineer/settings" element={<Blank />} />
            <Route path="/dataengineer/profile" element={<UserProfiles />} />

            {/* Business Metadata Pages */}
            <Route path="/dataengineer/sources" element={<SourcesPage />} />
            <Route path="/dataengineer/catalog" element={<CatalogPage />} />
            <Route path="/dataengineer/glossary" element={<GlossaryPage />} />
            <Route path="/dataengineer/expectations-jobs" element={<ExpectationsJobsPage />} />
          </Route>
          {/* MLISidebar Layout - Protected Routes */}
          <Route
            element={
              <ProtectedRoute requiredRole={["ANALYST", "DATA_ENGINEER", "ML", "MLI"]}>
                <MLILayout />
              </ProtectedRoute>
            }
          >
            {/* Machine Learning Pages */}
            <Route path="/ml/dashboard" element={<StatusOverviewPage />} />
            <Route path="/ml/models" element={<ModelsListPage />} />
            <Route path="/ml/models/create" element={<CreateModelPage />} />
            <Route path="/ml/models/:model_sk" element={<ModelDetailPage />} />
            <Route path="/ml/models/:model_sk/edit" element={<ModelDetailPage />} />
            <Route path="/ml/price-prediction" element={<PricePredictionPage />} />
            <Route path="/ml/recommendations" element={<RecommendationsPage />} />
            <Route path="/ml/sentiment" element={<SentimentAnalysisPage />} />
            <Route path="/ml/status" element={<StatusOverviewPage />} />

            {/* MLI Pages */}
            <Route path="/mli/dashboard" element={<MLIPage />} />
            <Route path="/mli/overview" element={<MLOverview />} />
            <Route path="/mli/price-intelligence" element={<PriceIntelligence />} />
            <Route path="/mli/demand-forecasting" element={<DemandSalesForecasting />} />
            <Route path="/mli/product-insights" element={<ProductMLInsights />} />
            <Route path="/mli/price-optimization" element={<Blank />} />
            <Route path="/mli/demand-forecast" element={<Blank />} />
            <Route path="/mli/sales-forecast" element={<Blank />} />
            <Route path="/mli/sales-trend" element={<Blank />} />
            <Route path="/mli/customer-segmentation" element={<Blank />} />
            <Route path="/mli/churn-prediction" element={<Blank />} />
            <Route path="/mli/recommendation-engine" element={<Blank />} />
            <Route path="/mli/model-management" element={<Blank />} />
            <Route path="/mli/data-sets" element={<Blank />} />
            <Route path="/mli/profile" element={<UserProfiles />} />
          </Route>
          {/* Customer Layout - Protected Routes */}
          <Route
            element={
              <ProtectedRoute requiredRole="CUSTOMER">
                <CustomerLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/customer" element={<CustomerPage />} />
            <Route path="/customer/dashboard" element={<VietnamElectronicsDashboard />} />
            <Route path="/customer/products/browse" element={<BasicTables />} />
            <Route path="/customer/products/favorites" element={<BasicTables />} />
            <Route path="/customer/products/recent" element={<BasicTables />} />
            <Route path="/customer/orders/history" element={<BasicTables />} />
            <Route path="/customer/orders/tracking" element={<BasicTables />} />
            <Route path="/customer/orders/returns" element={<BasicTables />} />
            <Route path="/customer/profile" element={<UserProfiles />} />
            <Route path="/customer/help" element={<Blank />} />
            <Route path="/customer/contact" element={<Blank />} />
            <Route path="/customer/faq" element={<Blank />} />
            <Route path="/customer/notifications/orders" element={<Alerts />} />
            <Route path="/customer/notifications/promotions" element={<Alerts />} />
            <Route path="/customer/notifications/account" element={<Alerts />} />
          </Route>

          {/* Public Layout */}
          <Route element={<PublicLayout />}>
            <Route
              path="/"
              element={<HomePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />}
            />
            <Route
              path="/home"
              element={<HomePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />}
            />
            <Route path="/about" element={<AboutPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            <Route path="/solutions" element={<SolutionsPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            <Route path="/contact" element={<ContactPage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            <Route path="/send-message" element={<SendMessagePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            <Route path="/explore" element={<ExplorePage navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={handleLogout} />} />
            <Route path="/analyst-home" element={<AnalystPage />} />
            <Route path="/customer-home" element={<CustomerPage />} />
            <Route path="/admin-home" element={<AdminPage />} />
            <Route path="/admin/users" element={<AdminUserManagement />} />
            <Route path="/admin/deleted-users" element={<DeletedUsersList onSelectUser={() => { }} />} />
            <Route path="/user/:userId" element={<UserDetailsWrapper />} />
            <Route path="/data-engineer" element={<DataEngineerDashboard />} />
          </Route>

          {/* Auth Layout */}
          <Route path="/" element={<Home />} />
          <Route path="/signin" element={<SignIn />} />
          <Route path="/signup" element={<SignUp />} />
          <Route path="/forgot-password" element={<ForgotPassword />} />
          <Route path="/reset-password" element={<ResetPassword />} />
          <Route path="/verify-code" element={<VerifyCode />} />
          <Route path="/password-reset-success" element={<PasswordResetSuccess />} />

          {/* Fallback Route */}
          <Route path="*" element={<NotFound />} />
        </Routes>
      </>
    );
  }

  return (
    <AuthProvider>
      <ToastProvider>
        <Router>
          <AppContent />
        </Router>
      </ToastProvider>
    </AuthProvider>
  );
}