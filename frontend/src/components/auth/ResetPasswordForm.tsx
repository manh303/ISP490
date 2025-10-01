import { useState, useEffect } from "react";
import { Link, useParams, useNavigate } from "react-router";
import { ChevronLeftIcon, EyeCloseIcon, EyeIcon } from "../../icons";
// import Label from "../form/Label";
import Input from "../form/input/InputField";

export default function ResetPasswordForm() {
  const [formData, setFormData] = useState({
    password: "",
    confirmPassword: ""
  });
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [errors, setErrors] = useState<{[key: string]: string}>({});
  const [passwordStrength, setPasswordStrength] = useState<{strength: number; label: string}>({ strength: 0, label: '' });
  const [isLoading, setIsLoading] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);
  const [tokenError, setTokenError] = useState("");
  
  const { token } = useParams();
  const navigate = useNavigate();

  const validateForm = () => {
    const newErrors: {[key: string]: string} = {};

    if (!formData.password) {
      newErrors.password = "Password is required";
    } else if (formData.password.length < 8) {
      newErrors.password = "Password must be at least 8 characters";
    }

    if (!formData.confirmPassword) {
      newErrors.confirmPassword = "Confirm password is required";
    } else if (formData.password !== formData.confirmPassword) {
      newErrors.confirmPassword = "Passwords do not match";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!validateForm()) return;

    setIsLoading(true);
    setTokenError("");

    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Simulate token validation
      if (!token || token === "invalid") {
        setTokenError("Invalid or expired reset link. Please request a new password reset.");
        return;
      }

      // Success
      setIsSuccess(true);
      
      // Redirect to sign in after 3 seconds
      setTimeout(() => {
        navigate("/signin");
      }, 3000);
      
    } catch (error) {
      setTokenError("Something went wrong. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: "" }));
    }
  };

  // Assess password strength based on length, uppercase, numbers and symbols
  const assessPasswordStrength = (password: string) => {
    if (!password) return { strength: 0, label: '' };
    let score = 0;
    if (password.length >= 8) score += 1;
    if (/[A-Z]/.test(password)) score += 1;
    if (/[0-9]/.test(password)) score += 1;
    if (/[^A-Za-z0-9]/.test(password)) score += 1;

    // Map score 0-4 to strength 1-4 (1=weak,4=strong)
    const strength = Math.max(1, score);
    const label =
      score <= 1 ? 'Weak' : score === 2 ? 'Fair' : score === 3 ? 'Good' : 'Strong';

    return { strength, label };
  };

  useEffect(() => {
    setPasswordStrength(assessPasswordStrength(formData.password));
  }, [formData.password]);

  // Show error if token is missing
  if (!token) {
    return (
      <div className="flex flex-col flex-1 w-full overflow-y-auto lg:w-1/2 no-scrollbar">
        {/* <div className="w-full max-w-md mx-auto mb-5 sm:pt-10">
          <Link
            to="/"
            className="inline-flex items-center text-sm text-gray-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
          >
            <ChevronLeftIcon className="size-5" />
            Back to dashboard
          </Link>
        </div> */}
        <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto text-center">
          <div className="mb-5">
            <div className="flex justify-center mb-4">
              <div className="flex items-center justify-center w-16 h-16 rounded-full bg-red-100 dark:bg-red-900/20">
                <svg
                  className="w-8 h-8 text-red-600 dark:text-red-400"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M6 18L18 6M6 6l12 12"
                  />
                </svg>
              </div>
            </div>
            <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
              Invalid Reset Link
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              This password reset link is invalid or has expired.
            </p>
          </div>
          <div className="space-y-4">
            <Link
              to="/forgot-password"
              className="flex items-center justify-center w-full px-4 py-3 text-sm font-medium text-white transition rounded-lg bg-brand-500 shadow-theme-xs hover:bg-brand-600"
            >
              Request New Reset Link
            </Link>
            <Link
              to="/signin"
              className="flex items-center justify-center w-full px-4 py-3 text-sm font-medium text-gray-700 transition bg-gray-100 rounded-lg hover:bg-gray-200 dark:bg-white/5 dark:text-white/90 dark:hover:bg-white/10"
            >
              Back to Sign In
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col flex-1 w-full overflow-y-auto lg:w-1/2 no-scrollbar">
      <div className="w-full max-w-md mx-auto mb-5 sm:pt-10">
        <Link
          to="/"
          className="inline-flex items-center text-sm text-gray-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
        >
          <ChevronLeftIcon className="size-5" />
          Back to dashboard
        </Link>
      </div>
      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        {!isSuccess ? (
          <div>
            <div className="mb-5 sm:mb-8">
              <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
                Create New Password
              </h1>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                 Please enter a new password below.
              </p>
            </div>

            {tokenError && (
              <div className="p-4 mb-5 text-sm text-red-800 bg-red-100 rounded-lg dark:bg-red-900/20 dark:text-red-400">
                {tokenError}
              </div>
            )}

            <div>
              <form onSubmit={handleSubmit}>
                <div className="space-y-5">
                  {/* New Password with strength meter */}
                  <div>
                    <div className="relative">
                      <Input
                        placeholder="Password"
                        type={showPassword ? "text" : "password"}
                        value={formData.password}
                        onChange={(e) => handleInputChange("password", e.target.value)}
                        className={errors.password ? "border-red-500" : ""}
                      />
                      <span
                        onClick={() => setShowPassword(!showPassword)}
                        className="absolute z-30 -translate-y-1/2 cursor-pointer right-4 top-1/2"
                      >
                        {showPassword ? (
                          <EyeIcon className="fill-gray-500 dark:fill-gray-400 size-5" />
                        ) : (
                          <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400 size-5" />
                        )}
                      </span>
                    </div>
                    {formData.password && (
                      <div className="mt-2">
                        <div className="w-full bg-gray-200 rounded-full h-1.5 dark:bg-gray-700 mb-2">
                          <div 
                            className={`h-1.5 rounded-full transition-all duration-300 ${
                              passwordStrength.strength === 1 ? 'bg-orange-500 w-1/4' :
                              passwordStrength.strength === 2 ? 'bg-yellow-500 w-2/4' :
                              passwordStrength.strength === 3 ? 'bg-blue-500 w-3/4' :
                              'bg-green-500 w-full'
                            }`} 
                          />
                        </div>
                        <div className="text-left">
                          <span className={`text-sm font-medium ${
                            passwordStrength.strength === 1 ? 'text-orange-500' :
                            passwordStrength.strength === 2 ? 'text-yellow-500' :
                            passwordStrength.strength === 3 ? 'text-blue-500' :
                            'text-green-500'
                          }`}>
                            Password strength: {passwordStrength.label}
                          </span>
                        </div>
                      </div>
                    )}
                    {errors.password && (
                      <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                        {errors.password}
                      </p>
                    )}
                  </div>

                  {/* Confirm Password */}
                  <div>
                    {/* <Label>
                      Confirm Password<span className="text-error-500">*</span>
                    </Label> */}
                    <div className="relative">
                      <Input
                        type={showConfirmPassword ? "text" : "password"}
                        placeholder="Confirm your new password"
                        value={formData.confirmPassword}
                        onChange={(e) => handleInputChange("confirmPassword", e.target.value)}
                        error={!!errors.confirmPassword}
                        hint={errors.confirmPassword}
                      />
                      <span
                        onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                        className="absolute z-30 -translate-y-1/2 cursor-pointer right-4 top-1/2"
                      >
                        {showConfirmPassword ? (
                          <EyeIcon className="fill-gray-500 dark:fill-gray-400 size-5" />
                        ) : (
                          <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400 size-5" />
                        )}
                      </span>
                    </div>
                  </div>

                  {/* Submit Button */}
                  <div>
                    <button
                      type="submit"
                      disabled={isLoading}
                      className="flex items-center justify-center w-full px-4 py-3 text-sm font-medium text-white transition rounded-lg bg-brand-500 shadow-theme-xs hover:bg-brand-600 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {isLoading ? (
                        <>
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
                          Updating Password...
                        </>
                      ) : (
                        "Update Password"
                      )}
                    </button>
                  </div>
                </div>
              </form>

              {/* <div className="mt-5">
                <p className="text-sm font-normal text-center text-gray-700 dark:text-gray-400 sm:text-start">
                  Remember your password? {""}
                  <Link
                    to="/signin"
                    className="text-brand-500 hover:text-brand-600 dark:text-brand-400"
                  >
                    Sign In
                  </Link>
                </p>
              </div> */}
            </div>
          </div>
        ) : (
          <div className="text-center">
            <div className="mb-5 sm:mb-8">
              <div className="flex justify-center mb-4">
                <div className="flex items-center justify-center w-16 h-16 rounded-full bg-green-100 dark:bg-green-900/20">
                  <svg
                    className="w-8 h-8 text-green-600 dark:text-green-400"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M5 13l4 4L19 7"
                    />
                  </svg>
                </div>
              </div>
              <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
                Password Updated!
              </h1>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Your password has been successfully updated. You will be redirected to sign in shortly.
              </p>
            </div>
            <div className="space-y-4">
              <Link
                to="/signin"
                className="flex items-center justify-center w-full px-4 py-3 text-sm font-medium text-white transition rounded-lg bg-brand-500 shadow-theme-xs hover:bg-brand-600"
              >
                Continue to Sign In
              </Link>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}