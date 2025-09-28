import { useState, useRef, useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router";
import { ChevronLeftIcon } from "../../icons";

export default function VerifyCodeForm() {
  const [code, setCode] = useState<string[]>(["", "", "", "", "", ""]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const [isResending, setIsResending] = useState(false);
  const [countdown, setCountdown] = useState(0);
  const inputRefs = useRef<(HTMLInputElement | null)[]>([]);
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const email = searchParams.get("email") || "user@example.com";

  // Countdown timer for resend button
  useEffect(() => {
    if (countdown > 0) {
      const timer = setTimeout(() => setCountdown(countdown - 1), 1000);
      return () => clearTimeout(timer);
    }
  }, [countdown]);

  // Auto-focus first input on mount
  useEffect(() => {
    inputRefs.current[0]?.focus();
  }, []);

  const handleInputChange = (index: number, value: string) => {
    // Only allow numbers
    if (!/^\d*$/.test(value)) return;

    const newCode = [...code];
    newCode[index] = value;
    setCode(newCode);
    setError(""); // Clear error when user types

    // Auto-move to next input
    if (value && index < 5) {
      inputRefs.current[index + 1]?.focus();
    }

    // Auto-submit when all 6 digits are entered
    if (newCode.every(digit => digit !== "") && newCode.join("").length === 6) {
      handleVerify(newCode.join(""));
    }
  };

  const handleKeyDown = (index: number, e: React.KeyboardEvent) => {
    // Handle backspace
    if (e.key === "Backspace") {
      if (!code[index] && index > 0) {
        // Move to previous input if current is empty
        inputRefs.current[index - 1]?.focus();
      } else {
        // Clear current input
        const newCode = [...code];
        newCode[index] = "";
        setCode(newCode);
      }
    }
    // Handle arrow keys
    else if (e.key === "ArrowLeft" && index > 0) {
      inputRefs.current[index - 1]?.focus();
    } else if (e.key === "ArrowRight" && index < 5) {
      inputRefs.current[index + 1]?.focus();
    }
    // Handle paste
    else if (e.key === "v" && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      navigator.clipboard.readText().then(text => {
        const digits = text.replace(/\D/g, "").slice(0, 6);
        if (digits.length === 6) {
          const newCode = digits.split("");
          setCode(newCode);
          inputRefs.current[5]?.focus();
          handleVerify(digits);
        }
      });
    }
  };

  const handleVerify = async (codeToVerify?: string) => {
    const verificationCode = codeToVerify || code.join("");
    
    if (verificationCode.length !== 6) {
      setError("Please enter all 6 digits");
      return;
    }

    setIsLoading(true);
    setError("");

    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Simulate verification
      if (verificationCode === "123456") {
        // Success - redirect to dashboard or next step
        navigate("/signin?verified=true");
      } else {
        setError("Invalid verification code. Please try again.");
        setCode(["", "", "", "", "", ""]);
        inputRefs.current[0]?.focus();
      }
    } catch (error) {
      setError("Something went wrong. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleResendCode = async () => {
    if (countdown > 0) return;

    setIsResending(true);
    setError("");

    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Start countdown (60 seconds)
      setCountdown(60);
      
      // Clear existing code
      setCode(["", "", "", "", "", ""]);
      inputRefs.current[0]?.focus();
      
    } catch (error) {
      setError("Failed to resend code. Please try again.");
    } finally {
      setIsResending(false);
    }
  };

  return (
    <div className="flex flex-col flex-1 w-full overflow-y-auto lg:w-1/2 no-scrollbar">
      <div className="w-full max-w-md mx-auto mb-5 sm:pt-10">
        <Link
          to="/signin"
          className="inline-flex items-center text-sm text-gray-500 transition-colors hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
        >
          <ChevronLeftIcon className="size-5" />
          Back to Login
        </Link>
      </div>
      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        <div className="text-center">
          <div className="mb-5 sm:mb-8">
            <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
              Verify Code
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Enter the 6-digit code sent to your email
            </p>
            <p className="mt-1 text-sm font-medium text-gray-700 dark:text-gray-300">
              {email}
            </p>
          </div>

          {error && (
            <div className="p-3 mb-5 text-sm text-red-800 bg-red-100 rounded-lg dark:bg-red-900/20 dark:text-red-400">
              {error}
            </div>
          )}

          <form onSubmit={(e) => { e.preventDefault(); handleVerify(); }}>
            <div className="space-y-6">
              {/* 6-digit code inputs */}
              <div className="flex justify-center gap-3">
                {code.map((digit, index) => (
                  <input
                    key={index}
                    ref={(el) => { inputRefs.current[index] = el; }}
                    type="text"
                    inputMode="numeric"
                    maxLength={1}
                    value={digit}
                    onChange={(e) => handleInputChange(index, e.target.value)}
                    onKeyDown={(e) => handleKeyDown(index, e)}
                    className={`w-12 h-12 text-center text-lg font-semibold border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
                      error
                        ? "border-red-500 focus:border-red-500 focus:ring-red-500/20 dark:border-red-500"
                        : "border-gray-300 focus:border-brand-500 focus:ring-brand-500/20 dark:border-gray-600 dark:focus:border-brand-400"
                    } bg-white dark:bg-gray-800 text-gray-900 dark:text-white`}
                    disabled={isLoading}
                  />
                ))}
              </div>

              {/* Verify Button */}
              <div>
                <button
                  type="submit"
                  disabled={isLoading || code.some(digit => !digit)}
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
                      Verifying...
                    </>
                  ) : (
                    "Verify"
                  )}
                </button>
              </div>

              {/* Resend Code */}
              <div className="text-center">
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Didn't receive the code?{" "}
                  <button
                    type="button"
                    onClick={handleResendCode}
                    disabled={countdown > 0 || isResending}
                    className={`${
                      countdown > 0 || isResending
                        ? "text-gray-400 cursor-not-allowed"
                        : "text-brand-500 hover:text-brand-600 dark:text-brand-400"
                    } transition-colors`}
                  >
                    {isResending ? (
                      "Sending..."
                    ) : countdown > 0 ? (
                      `Resend Code (${countdown}s)`
                    ) : (
                      "Resend Code"
                    )}
                  </button>
                </p>
              </div>
            </div>
          </form>

          {/* Demo hint */}
          <div className="mt-6 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
            <p className="text-xs text-blue-600 dark:text-blue-400">
              💡 <strong>Demo:</strong> Use code <code className="px-1 bg-blue-100 dark:bg-blue-800 rounded">123456</code> to test verification
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}