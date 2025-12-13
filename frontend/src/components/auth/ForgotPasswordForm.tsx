import { useState } from "react";
import { Link, useNavigate } from "react-router";
import Input from "../form/input/InputField";
// import authService from "../../services/authService";
import { useToast } from "../../contexts/ToastContext";
import { ChevronLeftIcon } from "../../icons";
import { authAPI } from "../../services/api";

export default function ForgotPasswordForm() {
  const [email, setEmail] = useState("");
  const [errors, setErrors] = useState({
    email: "",
    general: ""
  });
  const [isLoading, setIsLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const navigate = useNavigate();
  const { showToast } = useToast();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({ email: "", general: "" });

    // Basic validation
    if (!email.trim()) {
      setErrors({ email: "Email is required", general: "" });
      showToast("Please enter your email address", "error");
      return;
    }

    // More robust email format validation
    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    if (!emailRegex.test(email.trim())) {
      setErrors({ email: "Please enter a valid email address (e.g., user@example.com)", general: "" });
      showToast("Please enter a valid email address", "error");
      return;
    }

    setIsLoading(true);
    try {
      showToast("Sending recovery code...", "info", 2000);

      const response = await authAPI.forgotPassword({ email });

      if (response.success) {
        setSuccess(true);
        showToast(`✅ ${response.message}`, "success", 2000);

        sessionStorage.setItem('reset_email', email);

        // Navigate to verify-code page
        navigate(`/verify-code`);
      } else {
        setErrors({ email: "", general: response.message || "Unable to send password recovery code. Please try again." });
        showToast("❌ Failed to send reset code", "error");
      }
    } catch (error: any) {
      console.error('Forgot password error:', error);

      let errorMessage = 'Unable to send password recovery code. Please try again.';

      // Extract error message from axios response
      if (error?.response?.data?.detail) {
        // Handle both string and object detail formats
        const detail = error.response.data.detail;
        errorMessage = typeof detail === 'string' ? detail : detail.message || JSON.stringify(detail);
      } else if (error?.response?.data?.message) {
        errorMessage = error.response.data.message;
      } else if (error?.message) {
        errorMessage = error.message;
      }

      setErrors({ email: "", general: `❌ ${errorMessage}` });
      showToast(`❌ ${errorMessage}`, "error");
    } finally {
      setIsLoading(false);
    }
  };
  return (
    <div className="flex flex-col flex-1 justify-center items-center min-h-screen">
      <div className="w-full max-w-md pt-10 mx-auto">
        <Link
          to="/"
          className="inline-flex items-center text-sm text-gray-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
        >
          <ChevronLeftIcon className="size-5" />
          Back to Home
        </Link>
      </div>

      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        <div>
          <div className="mb-5 sm:mb-8 flex justify-center w-full">
            <div className="flex items-center justify-center w-20 h-20 rounded-full bg-blue-50 mx-auto">
              {/* mail icon */}
              <svg
                width="34"
                height="34"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className="text-blue-600 dark:text-blue-400"
              >
                <path d="M3 8.5L12 13L21 8.5" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                <rect x="3" y="5" width="18" height="14" rx="2" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
          </div>
          <div className="mb-4 text-center">
            <h1 className="mb-2 font-semibold text-gray-900 dark:text-white text-2xl">
              {success ? "Check Your Email!" : "Forgot Password?"}
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {success
                ? "We have sent a password reset code to your email address. Please check your inbox and follow the instructions."
                : "Enter your email to receive a recovery code."
              }
            </p>
          </div>
          {!success && (
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
                      className={`w-full ${errors.email ? "border-red-500" : ""}`}
                      disabled={isLoading}
                    />
                    {errors.email && (
                      <p className="mt-1 text-sm text-red-600 dark:text-red-400 text-left">
                        {errors.email}
                      </p>
                    )}
                  </div>
                  {/* Error Message */}
                  {errors.general && (
                    <div className="flex items-center gap-2 p-3 text-sm text-red-700 bg-red-100 border border-red-200 rounded-lg dark:bg-red-900/20 dark:text-red-400 dark:border-red-800">
                      <svg className="w-4 h-4 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                      </svg>
                      <span className="font-medium">{errors.general}</span>
                    </div>
                  )}
                  <div>
                    <button
                      type="submit"
                      disabled={isLoading}
                      className="w-full px-4 py-3 text-sm font-medium text-white rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
                    >
                      {isLoading ? (
                        <div className="flex items-center justify-center">
                          <svg className="w-4 h-4 mr-2 animate-spin" viewBox="0 0 24 24">
                            <circle
                              className="opacity-25"
                              cx="12"
                              cy="12"
                              r="10"
                              stroke="currentColor"
                              strokeWidth="4"
                            />
                            <path
                              className="opacity-75"
                              fill="currentColor"
                              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                            />
                          </svg>
                          Sending...
                        </div>
                      ) : (
                        "Send Recovery Code"
                      )}
                    </button>
                  </div>
                </div>
              </form>
            </div>
          )}
          {success && (
            <div className="space-y-4">
              <div className="p-4 bg-green-100 border border-green-200 rounded-lg dark:bg-green-900/20 dark:border-green-800">
                <div className="flex items-center justify-center">
                  <svg className="w-6 h-6 text-green-600 dark:text-green-400" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                  </svg>
                  <span className="ml-2 text-sm font-medium text-green-800 dark:text-green-400">
                    Password recovery code has been sent successfully!
                  </span>
                </div>
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Redirecting to sign-in page in 3 seconds...
              </p>
            </div>
          )}
          <div className="mt-6 text-center">
            <Link to="/signin" className="inline-flex items-center text-sm text-blue-600 hover:text-blue-700">
              <svg className="mr-2" width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M15 18l-6-6 6-6" stroke="#2563EB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              Back to Sign In
            </Link>
          </div>
        </div>
      </div>
    </div >
  );
}