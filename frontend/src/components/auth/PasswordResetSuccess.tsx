import { Link } from "react-router";

export default function PasswordResetSuccess() {
  return (
    <div className="flex items-center justify-center min-h-screen p-6 bg-gray-50 dark:bg-slate-900">
      <div className="w-full max-w-xl bg-white dark:bg-slate-800 rounded-xl shadow-lg p-10 text-center">
        <div className="flex justify-center mb-6">
          <div className="w-20 h-20 rounded-full bg-green-100 flex items-center justify-center">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="w-8 h-8 text-green-600"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth={2}
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M20 6L9 17l-5-5" stroke="#059669" />
            </svg>
          </div>
        </div>

        <h1 className="mb-4 text-2xl font-semibold text-gray-900 dark:text-white">Password reset successful</h1>

        <p className="mb-8 text-sm text-gray-600 dark:text-gray-300">
          Your password has been reset successfully. You can now login with your new password.
        </p>

        <div className="max-w-sm mx-auto">
          <Link
            to="/signin"
            className="block w-full text-center px-6 py-3 rounded-lg bg-emerald-500 hover:bg-emerald-600 text-white font-medium shadow-sm"
          >
            Back to Login
          </Link>
        </div>
      </div>
    </div>
  );
}
