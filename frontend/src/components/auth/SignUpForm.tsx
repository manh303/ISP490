import { useState } from "react";
import { Link, useNavigate } from "react-router";
import { ChevronLeftIcon, EyeCloseIcon, EyeIcon } from "../../icons";
// import Label from "../form/Label";
import Input from "../form/input/InputField";
// import Checkbox from "../form/input/Checkbox";
import Button from "../ui/button/Button";

export default function SignUpForm() {
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isChecked, setIsChecked] = useState(false);
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    password: "",
    confirmPassword: ""
  });
  const [errors, setErrors] = useState({
    name: "",
    email: "",
    password: "",
    confirmPassword: "",
    general: ""
  });
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  // Password strength calculation
  const getPasswordStrength = (password: string) => {
    if (!password) return { strength: 0, label: "" };
    
    let score = 0;
    const checks = {
      length: password.length >= 8,
      lowercase: /[a-z]/.test(password),
      uppercase: /[A-Z]/.test(password),
      number: /\d/.test(password),
      special: /[!@#$%^&*(),.?":{}|<>]/.test(password)
    };
    
    Object.values(checks).forEach(check => {
      if (check) score++;
    });
    
    if (score <= 2) return { strength: 1, label: "Fair" };
    if (score <= 3) return { strength: 2, label: "Good" };
    if (score <= 4) return { strength: 3, label: "Strong" };
    return { strength: 4, label: "Very Strong" };
  };

  const passwordStrength = getPasswordStrength(formData.password);
  return (
    <div className="flex flex-col flex-1 w-full overflow-y-auto lg:w-1/2 no-scrollbar">
      <div className="w-full max-w-md mx-auto mb-5 sm:pt-10">
      </div>
      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        <div>
          <div className="mb-5 sm:mb-8 text-center">
            <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
              Create Account
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Join us today and start your journey
            </p>
          </div>
          <div>
           
            <form onSubmit={async (e) => {
              e.preventDefault();
              setErrors({ name: "", email: "", password: "", confirmPassword: "", general: "" });
              
              // Basic validation
              let hasErrors = false;
              const newErrors = { name: "", email: "", password: "", confirmPassword: "", general: "" };
              
              if (!formData.name.trim()) {
                newErrors.name = "Name is required";
                hasErrors = true;
              }
              
              if (!formData.email.trim()) {
                newErrors.email = "Email is required";
                hasErrors = true;
              } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
                newErrors.email = "Please enter a valid email address";
                hasErrors = true;
              }
              
              if (!formData.password) {
                newErrors.password = "Password is required";
                hasErrors = true;
              } else if (formData.password.length < 8) {
                newErrors.password = "Password must be at least 8 characters";
                hasErrors = true;
              }
              
              if (formData.password !== formData.confirmPassword) {
                newErrors.confirmPassword = "Passwords do not match";
                hasErrors = true;
              }
              
              if (!isChecked) {
                newErrors.general = "You must agree to the Terms and Conditions";
                hasErrors = true;
              }
              
              if (hasErrors) {
                setErrors(newErrors);
                return;
              }
              
              setIsLoading(true);
              try {
                // Simulate API call
                await new Promise(resolve => setTimeout(resolve, 2000));
                
                // Navigate to verify code page with email
                navigate(`/verify-code?email=${encodeURIComponent(formData.email)}`);
              } catch (error) {
                setErrors({ ...newErrors, general: "Something went wrong. Please try again." });
              } finally {
                setIsLoading(false);
              }
            }}>
              <div className="space-y-5">
                {/* <!-- Name --> */}
                <div>
                  {/* <Label>
                    Name<span className="text-error-500">*</span>
                  </Label> */}
                  <Input
                    type="text"
                    placeholder="Full Name"
                    value={formData.name}
                    onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                    className={errors.name ? "border-red-500" : ""}
                  />
                  {errors.name && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.name}
                    </p>
                  )}
                </div>
                
                {/* <!-- Email --> */}
                <div>
                  {/* <Label>
                    Email<span className="text-error-500">*</span>
                  </Label> */}
                  <Input
                    type="email"
                    placeholder="Email"
                    value={formData.email}
                    onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
                    className={errors.email ? "border-red-500" : ""}
                  />
                  {errors.email && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.email}
                    </p>
                  )}
                </div>
                
                {/* <!-- Password --> */}
                <div>
                  {/* <Label>
                    Password<span className="text-error-500">*</span>
                  </Label> */}
                  <div className="relative">
                    <Input
                      placeholder="Password"
                      type={showPassword ? "text" : "password"}
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
                  {formData.password && (
                    <div className="mt-2">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-gray-600 dark:text-gray-400">Password strength:</span>
                        <span className={`text-xs font-medium ${
                          passwordStrength.strength === 1 ? 'text-orange-500' :
                          passwordStrength.strength === 2 ? 'text-yellow-500' :
                          passwordStrength.strength === 3 ? 'text-blue-500' :
                          'text-green-500'
                        }`}>
                          {passwordStrength.label}
                        </span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-1.5 dark:bg-gray-700">
                        <div 
                          className={`h-1.5 rounded-full transition-all duration-300 ${
                            passwordStrength.strength === 1 ? 'bg-orange-500 w-1/4' :
                            passwordStrength.strength === 2 ? 'bg-yellow-500 w-2/4' :
                            passwordStrength.strength === 3 ? 'bg-blue-500 w-3/4' :
                            'bg-green-500 w-full'
                          }`}
                        />
                      </div>
                    </div>
                  )}
                  {errors.password && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.password}
                    </p>
                  )}
                </div>
                
                {/* <!-- Confirm Password --> */}
                <div>
                  {/* <Label>
                    Confirm Password<span className="text-error-500">*</span>
                  </Label> */}
                  <div className="relative">
                    <Input
                      placeholder="Confirm Password"
                      type={showConfirmPassword ? "text" : "password"}
                      value={formData.confirmPassword}
                      onChange={(e) => setFormData(prev => ({ ...prev, confirmPassword: e.target.value }))}
                      className={errors.confirmPassword ? "border-red-500" : ""}
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
                  {errors.confirmPassword && (
                    <p className="mt-1 text-sm text-red-600 dark:text-red-400">
                      {errors.confirmPassword}
                    </p>
                  )}
                </div>
                {/* <!-- Checkbox --> */}
                {/* <div className="flex items-start gap-3">
                  <Checkbox
                    className="w-5 h-5 mt-0.5"
                    checked={isChecked}
                    onChange={setIsChecked}
                  />
                  <p className="text-sm font-normal text-gray-500 dark:text-gray-400">
                    By creating an account means you agree to the{" "}
                    <span className="text-gray-800 dark:text-white/90">
                      Terms and Conditions,
                    </span>{" "}
                    and our{" "}
                    <span className="text-gray-800 dark:text-white">
                      Privacy Policy
                    </span>
                  </p>
                </div> */}
                
                {/* Error Message */}
                {errors.general && (
                  <div className="flex items-center gap-2 p-3 text-sm text-red-700 bg-red-100 border border-red-200 rounded-lg dark:bg-red-900/20 dark:text-red-400 dark:border-red-800">
                    <svg className="w-4 h-4 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                    </svg>
                    {errors.general}
                  </div>
                )}
                
                {/* <!-- Button --> */}
                <div>
                  <Button 
                    className="w-full"
                    size="sm"
                    disabled={isLoading}
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
                        Creating Account...
                      </>
                    ) : (
                      "Create Account"
                    )}
                  </Button>
                </div>
              </div>
            </form>

            <div className="mt-5">
              <p className="text-sm font-normal text-center text-gray-700 dark:text-gray-400">
                Already have an account? {""}
                <Link
                  to="/signin"
                  className="text-brand-500 hover:text-brand-600 dark:text-brand-400"
                >
                  Login
                </Link>
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
