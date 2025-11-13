// /**
//  * ML Insights Route Configuration Example
//  * 
//  * Add this to your main router file (e.g., App.tsx or routes.tsx)
//  */

// import { Route, Routes } from 'react-router-dom';
// import MLInsights from '@/pages/MLInsights';

// // Example 1: Basic Route
// export const MLInsightsRoutes = () => {
//   return (
//     <Routes>
//       <Route path="/ml-insights" element={<MLInsights />} />
//     </Routes>
//   );
// };

// // Example 2: Protected Route with Layout
// import { ProtectedRoute } from '@/components/auth/ProtectedRoute';
// import Layout from '@/layout/Layout';

// export const ProtectedMLInsightsRoute = () => {
//   return (
//     <Route 
//       path="/ml-insights" 
//       element={
//         <ProtectedRoute>
//           <Layout>
//             <MLInsights />
//           </Layout>
//         </ProtectedRoute>
//       } 
//     />
//   );
// };

// // Example 3: Add to existing routes
// export const AppRoutes = () => {
//   return (
//     <Routes>
//       <Route path="/" element={<Home />} />
//       <Route path="/dashboard" element={<Dashboard />} />
//       <Route path="/analytics" element={<Analytics />} />
      
//       {/* ML Insights Route */}
//       <Route path="/ml-insights" element={<MLInsights />} />
      
//       {/* Other routes */}
//       <Route path="*" element={<NotFound />} />
//     </Routes>
//   );
// };

// // Example 4: Add to Navigation Menu
// export const NavigationMenu = () => {
//   return (
//     <nav>
//       <ul>
//         <li>
//           <Link to="/dashboard">Dashboard</Link>
//         </li>
//         <li>
//           <Link to="/analytics">Analytics</Link>
//         </li>
//         <li>
//           <Link to="/ml-insights">
//             🤖 ML Insights
//           </Link>
//         </li>
//       </ul>
//     </nav>
//   );
// };

// // Example 5: Sidebar Menu Item
// export const SidebarMenuItem = {
//   label: 'ML Insights',
//   icon: '🤖',
//   path: '/ml-insights',
//   description: 'AI-powered business analytics',
//   requiresAuth: true,
//   roles: ['admin', 'manager'], // Optional: restrict by role
// };
