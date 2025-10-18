import { useState } from "react";
import { ArrowLeft } from "lucide-react";
import type { Page } from "../App";

interface LoginPageProps {
  onLogin: () => void;
  navigateTo: (page: Page) => void;
}

export function LoginPage({ onLogin, navigateTo }: LoginPageProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onLogin();
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-blue-50 flex items-center justify-center p-8">
      {/* Back Button */}
      <button
        onClick={() => navigateTo("home")}
        className="absolute top-8 left-8 flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
      >
        <ArrowLeft className="w-5 h-5" />
        <span>Quay lại trang chủ</span>
      </button>

      {/* Login Card */}
      <div className="w-full max-w-[448px] h-[464px]">
        <div className="bg-white overflow-clip relative rounded-[16px] shadow-[0px_20px_25px_-5px_rgba(0,0,0,0.1),0px_8px_10px_-6px_rgba(0,0,0,0.1)] size-full">
          {/* Header */}
          <div className="absolute content-stretch flex flex-col gap-[8px] items-start left-[32px] right-[32px] top-[32px]">
            <div className="content-stretch flex flex-col items-center relative shrink-0 w-full">
              <div className="flex flex-col font-['Inter:Bold',_sans-serif] font-bold justify-center leading-[0] not-italic relative shrink-0 text-[29.297px] text-center text-gray-900 w-full">
                <p className="leading-[36px]">Welcome Back</p>
              </div>
            </div>
            <div className="content-stretch flex flex-col items-center relative shrink-0 w-full">
              <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[15px] text-center text-gray-600 w-full">
                <p className="leading-[24px]">Please sign in to your account</p>
              </div>
            </div>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="absolute h-[200px] left-[32px] right-[32px] top-[132px]">
            {/* Username Input */}
            <div className="absolute bg-white left-0 right-0 rounded-[8px] top-0">
              <div className="box-border content-stretch flex items-start justify-center overflow-clip pb-[16px] pt-[17px] px-[18px] relative rounded-[inherit] w-full">
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="Admin"
                  className="box-border content-stretch flex flex-col items-start overflow-clip pb-[2px] pt-px px-0 relative shrink-0 w-full font-['Inter:Regular',_sans-serif] font-normal leading-[0] not-italic text-[15.25px] text-gray-900 bg-transparent border-none outline-none"
                  required
                />
              </div>
              <div aria-hidden="true" className="absolute border-2 border-gray-200 border-solid inset-0 pointer-events-none rounded-[8px]" />
            </div>

            {/* Password Input */}
            <div className="absolute bg-white left-0 right-0 rounded-[8px] top-[76px]">
              <div className="box-border content-stretch flex items-start justify-center overflow-clip pb-[16px] pt-[17px] px-[18px] relative rounded-[inherit] w-full">
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="admin1234@"
                  className="box-border content-stretch flex flex-col items-start overflow-clip pb-[2px] pt-px px-0 relative shrink-0 w-full font-['Inter:Regular',_sans-serif] font-normal leading-[0] not-italic text-[15px] text-gray-900 bg-transparent border-none outline-none"
                  required
                />
              </div>
              <div aria-hidden="true" className="absolute border-2 border-gray-200 border-solid inset-0 pointer-events-none rounded-[8px]" />
            </div>

            {/* Login Button */}
            <button
              type="submit"
              className="absolute bg-blue-600 hover:bg-blue-700 transition-colors box-border content-stretch flex items-center justify-center left-0 px-0 py-[12px] rounded-[8px] top-[152px] w-full cursor-pointer"
            >
              <div className="flex flex-col font-['Inter:Semi_Bold',_sans-serif] font-semibold justify-center leading-[0] not-italic relative shrink-0 text-[16px] text-center text-nowrap text-white">
                <p className="leading-[24px] whitespace-pre">Login</p>
              </div>
            </button>
          </form>

          {/* Footer Links */}
          <div className="absolute box-border content-stretch flex flex-col gap-[17px] items-center left-[32px] pb-0 pt-[3px] px-0 right-[32px] top-[356px]">
            {/* Forgot Password */}
            <button
              type="button"
              onClick={() => navigateTo("forgot-password")}
              className="content-stretch flex items-center justify-center relative shrink-0 hover:opacity-80 transition-opacity"
            >
              <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.234px] text-blue-600 text-center text-nowrap">
                <p className="leading-[20px] whitespace-pre">Forgot Password?</p>
              </div>
            </button>

            {/* Create Account */}
            <div className="content-stretch flex items-start justify-center relative shrink-0 w-full">
              <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.563px] text-center text-gray-600 text-nowrap">
                <p className="leading-[20px] whitespace-pre">{`Don't have an account? `}</p>
              </div>
              <button
                type="button"
                onClick={() => navigateTo("register")}
                className="content-stretch flex items-center justify-center relative shrink-0 hover:opacity-80 transition-opacity"
              >
                <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.234px] text-blue-600 text-center text-nowrap">
                  <p className="leading-[20px] whitespace-pre">Create Account</p>
                </div>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
