import { useState } from "react";
import { Link } from "react-router";
// icons/Label removed as they are not used in the simplified UI
import Input from "../form/input/InputField";

export default function ForgotPasswordForm() {
  const [email, setEmail] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
  // Xử lý logic gửi email reset code ở đây
  };
  return (
    <div className="flex items-center justify-center min-h-screen p-6 bg-white dark:bg-slate-900">
      <div className="w-full max-w-sm">
        <div className="bg-white dark:bg-slate-800 border border-gray-100 dark:border-transparent rounded-xl shadow-lg p-8 text-center">
          <div className="flex justify-center mb-6">
            <div className="flex items-center justify-center w-20 h-20 rounded-full bg-blue-50">
              {/* mail icon */}
              <svg
                width="34"
                height="34"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className="text-blue-600"
              >
                <path d="M3 8.5L12 13L21 8.5" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                <rect x="3" y="5" width="18" height="14" rx="2" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
            </div>
          </div>
          <div className="mb-4">
            <h1 className="mb-2 font-semibold text-gray-900 dark:text-white text-2xl">Forgot Password?</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">Enter your email address and we'll send you a reset code.</p>
          </div>
          <div>
            <form onSubmit={handleSubmit}>
              <div className="space-y-4">
                <div>
                  <Input
                    type="email"
                    id="email"
                    name="email"
                    placeholder="Email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="w-full"
                  />
                </div>
                <div>
                  <button
                    type="submit"
                    className="w-full px-4 py-3 text-sm font-medium text-white rounded-lg bg-blue-600 hover:bg-blue-700"
                  >
                    Send Reset Code
                  </button>
                </div>
              </div>
            </form>
          </div>
          <div className="mt-6">
            <Link to="/signin" className="inline-flex items-center text-sm text-blue-600 hover:text-blue-700">
              <svg className="mr-2" width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M15 18l-6-6 6-6" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              Back to Login
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}