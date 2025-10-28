import { useState } from "react";
import { Link, useNavigate } from "react-router";
import Input from "../form/input/InputField";
import { authAPI } from "../../services/api";
import { useToast } from "../../contexts/ToastContext";
import { ChevronLeftIcon } from "../../icons";

export default function ForgotPasswordForm() {
  const [email, setEmail] = useState("");
  const [otp, setOtp] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [errors, setErrors] = useState({
    email: "",
    otp: "",
    password: "",
    confirmPassword: "",
    general: ""
  });
  const [isLoading, setIsLoading] = useState(false);
  const [step, setStep] = useState<"email" | "otp">("email");
  const navigate = useNavigate();
  const { showToast } = useToast();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (step === "email") {
      setErrors({ email: "", otp: "", password: "", confirmPassword: "", general: "" });
      if (!email.trim()) {
        setErrors(prev => ({ ...prev, email: "Email is required" }));
        showToast("Please enter your email address", "error");
        return;
      }
      if (!/\S+@\S+\.\S+/.test(email)) {
        setErrors(prev => ({ ...prev, email: "Please enter a valid email address" }));
        showToast("Please enter a valid email address", "error");
        return;
      }
      setIsLoading(true);
      try {
        showToast("Sending reset code...", "info", 2000);
        const response = await authAPI.forgotPassword({ email });
        if (response.success) {
          setStep("otp");
          showToast(`✅ ${response.message}`, "success", 6000);
        } else {
          setErrors(prev => ({ ...prev, general: response.message || "Failed to send reset code. Please try again." }));
          showToast("❌ Failed to send reset code", "error");
        }
      } catch (error: any) {
        console.error('Forgot password error:', error);
        const errorMessage = error?.message || 'Failed to send reset code. Please try again.';
        setErrors(prev => ({ ...prev, general: `❌ ${errorMessage}` }));
        showToast(errorMessage, "error");
      } finally {
        setIsLoading(false);
      }
    } else {
      // step === 'otp'
      setErrors({ email: "", otp: "", password: "", confirmPassword: "", general: "" });
      if (!otp.trim()) {
        setErrors(prev => ({ ...prev, otp: "OTP code is required" }));
        return;
      }
      if (newPassword.length < 8) {
        setErrors(prev => ({ ...prev, password: "Password must be at least 8 characters" }));
        return;
      }
      if (newPassword !== confirmPassword) {
        setErrors(prev => ({ ...prev, confirmPassword: "Passwords do not match" }));
        return;
      }
      setIsLoading(true);
      try {
        const resp = await authAPI.resetPassword({ email, otp, new_password: newPassword } as any);
        const ok = (resp as any)?.success ?? false;
        if (ok) {
          showToast("✅ Password reset successfully. Please sign in.", "success", 4000);
          navigate('/signin');
        } else {
          const msg = (resp as any)?.message || 'Reset password failed';
          setErrors(prev => ({ ...prev, general: msg }));
          showToast(`❌ ${msg}`, "error");
        }
      } catch (error: any) {
        const msg = error?.response?.data?.detail || error?.message || 'Reset password failed';
        setErrors(prev => ({ ...prev, general: `❌ ${msg}` }));
        showToast(`❌ ${msg}`, "error");
      } finally {
        setIsLoading(false);
      }
    }
  };
  return (
    <div className="flex items-center justify-center min-h-screen p-6 bg-white dark:bg-slate-900">

     {/* Back to home link */}
     <div className="w-full max-w-md mx-auto mb-5 sm:pt-10">
       <Link
         to="/"
         className="inline-flex items-center text-sm text-gray-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
       >
         <ChevronLeftIcon className="size-5" />
         Back to home
       </Link>
     </div>

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
            <h1 className="mb-2 font-semibold text-gray-900 dark:text-white text-2xl">
              {step === "email" ? "Forgot Password?" : "Enter Verification Code"}
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {step === "email"
                ? "Enter your email address and we'll send you a reset code."
                : "Enter the code sent to your email and set a new password."
              }
            </p>
          </div>
          {step === "email" && (
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
                        "Send Reset Code"
                      )}
                    </button>
                  </div>
                </div>
              </form>
            </div>
          )}
          {step === "otp" && (
            <div>
              <form onSubmit={handleSubmit}>
                <div className="space-y-4 text-left">
                  <div>
                    <Input
                      type="text"
                      id="otp"
                      name="otp"
                      placeholder="Verification code"
                      value={otp}
                      onChange={(e) => setOtp(e.target.value)}
                      className={`w-full ${errors.otp ? "border-red-500" : ""}`}
                      disabled={isLoading}
                    />
                    {errors.otp && (
                      <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                        {errors.otp}
                      </p>
                    )}
                  </div>
                  <div>
                    <Input
                      type="password"
                      id="new-password"
                      name="new-password"
                      placeholder="New password"
                      value={newPassword}
                      onChange={(e) => setNewPassword(e.target.value)}
                      className={`w-full ${errors.password ? "border-red-500" : ""}`}
                      disabled={isLoading}
                    />
                    {errors.password && (
                      <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                        {errors.password}
                      </p>
                    )}
                  </div>
                  <div>
                    <Input
                      type="password"
                      id="confirm-password"
                      name="confirm-password"
                      placeholder="Confirm new password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      className={`w-full ${errors.confirmPassword ? "border-red-500" : ""}`}
                      disabled={isLoading}
                    />
                    {errors.confirmPassword && (
                      <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                        {errors.confirmPassword}
                      </p>
                    )}
                  </div>

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
                      {isLoading ? "Processing..." : "Reset Password"}
                    </button>
                  </div>
                </div>
              </form>
            </div>
          )}
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