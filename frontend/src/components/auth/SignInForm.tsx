import { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router";
import { ChevronLeftIcon, EyeCloseIcon, EyeIcon } from "../../icons";
// import Label from "../form/Label";
import Input from "../form/input/InputField";
// import Checkbox from "../form/input/Checkbox";
import Button from "../ui/button/Button";
import { useAuth } from "../../contexts/AuthContext";
import authService from "../../services/authService";

export default function SignInForm() {
  const [showPassword, setShowPassword] = useState(false);
  const [rememberMe, setRememberMe] = useState(false);
  const [formData, setFormData] = useState({
    username: "",
    password: ""
  });
  const [errors, setErrors] = useState({
    username: "",
    password: "",
    general: ""
  });
  const [testCredentials, setTestCredentials] = useState<Array<{
    username: string;
    password: string;
    role: string;
    description: string;
  }>>([]);
  const [showTestCredentials, setShowTestCredentials] = useState(false);

  const { login, state } = useAuth();
  const navigate = useNavigate();

  // Load test credentials on component mount
  useEffect(() => {
    const loadTestCredentials = async () => {
      try {
        const credentials = await authService.getTestCredentials();
        setTestCredentials(credentials);
      } catch (error) {
        console.error('Failed to load test credentials:', error);
      }
    };

    loadTestCredentials();
  }, []);

  // Redirect if already authenticated
  useEffect(() => {
    if (state.isAuthenticated) {
      navigate('/dashboard');
    }
  }, [state.isAuthenticated, navigate]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({ username: "", password: "", general: "" });

    // Basic validation
    if (!formData.username || !formData.password) {
      setErrors({
        username: !formData.username ? "Username is required" : "",
        password: !formData.password ? "Password is required" : "",
        general: "Please fill in all fields"
      });
      return;
    }

    try {
      await login({
        username: formData.username,
        password: formData.password,
        remember_me: rememberMe
      });
      // Navigation will happen automatically via useEffect
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Login failed';
      setErrors({
        username: "",
        password: "",
        general: errorMessage
      });
    }
  };

  const fillTestCredentials = (username: string, password: string) => {
    setFormData({ username, password });
    setShowTestCredentials(false);
  };

  return (
    <div className="flex flex-col flex-1">
      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        <div>
          <div className="mb-5 sm:mb-8 text-center">
            <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
              Welcome Back
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Please sign in to your account
            </p>
          </div>

          {/* Error Message */}
          {errors.general && (
            <div className="flex items-center gap-2 p-3 mb-4 text-sm text-red-700 bg-red-100 border border-red-200 rounded-lg dark:bg-red-900/20 dark:text-red-400 dark:border-red-800">
              <svg className="w-4 h-4 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
              {errors.general}
            </div>
          )}
          <div>
            <form onSubmit={handleSubmit}>
              <div className="space-y-6">
                <div>
                  <Input
                    type="text"
                    placeholder="Username"
                    value={formData.username}
                    onChange={(e) => setFormData(prev => ({ ...prev, username: e.target.value }))}
                    className={errors.username ? "border-red-500" : ""}
                  />
                  {errors.username && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.username}
                    </p>
                  )}
                </div>
                <div>
                  {/* <Label>
                    Password <span className="text-error-500">*</span>{" "}
                  </Label> */}
                  <div className="relative">
                    <Input
                      type={showPassword ? "text" : "password"}
                      placeholder="••••••"
                      value={formData.password}
                      onChange={(e) => setFormData(prev => ({ ...prev, password: e.target.value }))}
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
                  {errors.password && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.password}
                    </p>
                  )}
                </div>
                <div className="flex items-center mb-4">
                  <input
                    type="checkbox"
                    id="remember-me"
                    checked={rememberMe}
                    onChange={(e) => setRememberMe(e.target.checked)}
                    className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500 dark:focus:ring-blue-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                  />
                  <label htmlFor="remember-me" className="ml-2 text-sm font-medium text-gray-900 dark:text-gray-300">
                    Remember me
                  </label>
                </div>
                <div>
                  <Button
                    className="w-full"
                    size="sm"
                    disabled={state.isLoading}
                  >
                    {state.isLoading ? (
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
                        Signing in...
                      </>
                    ) : (
                      "Login"
                    )}
                  </Button>
                </div>
                <div className="flex justify-center">
                  <Link
                    to="/forgot-password"
                    className="text-sm text-blue-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
                  >
                    Forgot Password?
                  </Link>
                </div>
                
              </div>
            </form>

            {/* Test Credentials Section */}
            <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border">
              <button
                type="button"
                onClick={() => setShowTestCredentials(!showTestCredentials)}
                className="w-full flex items-center justify-between text-sm font-medium text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-gray-100"
              >
                <span>🧪 Test Credentials</span>
                <svg
                  className={`w-4 h-4 transition-transform ${showTestCredentials ? 'rotate-180' : ''}`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>

              {showTestCredentials && (
                <div className="mt-3 space-y-2">
                  <p className="text-xs text-gray-600 dark:text-gray-400">
                    Click any credential below to auto-fill the form:
                  </p>
                  {testCredentials.map((cred, index) => (
                    <div
                      key={index}
                      onClick={() => fillTestCredentials(cred.username, cred.password)}
                      className="cursor-pointer p-2 bg-white dark:bg-gray-700 rounded border hover:border-blue-300 dark:hover:border-blue-600 transition-colors"
                    >
                      <div className="flex justify-between items-center">
                        <div>
                          <p className="text-sm font-medium text-gray-900 dark:text-gray-100">
                            {cred.username}
                          </p>
                          <p className="text-xs text-gray-500 dark:text-gray-400">
                            {cred.description}
                          </p>
                        </div>
                        <span
                          className={`px-2 py-1 text-xs rounded-full ${
                            cred.role === 'admin'
                              ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                              : cred.role === 'manager'
                              ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
                              : cred.role === 'analyst'
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                              : 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
                          }`}
                        >
                          {cred.role}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="mt-5">
              <p className="text-sm font-normal text-center text-gray-700 dark:text-gray-400">
                Don&apos;t have an account? {""}
                <Link
                  to="/signup"
                  className="text-brand-500 hover:text-brand-600 dark:text-brand-400"
                >
                  Create Account
                </Link>
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
