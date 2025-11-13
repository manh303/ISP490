import React, { useState } from "react";
import { DropdownItem } from "../ui/dropdown/DropdownItem";
import { Dropdown } from "../ui/dropdown/Dropdown";
import { useNavigate } from "react-router"; // <-- đúng package
import { useAuth } from "../../contexts/AuthContext";
import { useToast } from "../../contexts/ToastContext";

export default function UserDropdown() {
  const [isOpen, setIsOpen] = useState(false);

  // ✅ AuthContext của bạn trả { user, logout, ... } chứ không phải { state }
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const { showToast } = useToast();

  const toggleDropdown = () => setIsOpen((v) => !v);
  const closeDropdown = () => setIsOpen(false);

  const handleLogout = async () => {
    try {
      showToast("Signing out...", "info", 2000);
      await logout();
      closeDropdown();
      showToast("✅ Successfully signed out. See you soon!", "success", 3000);
      setTimeout(() => navigate("/signin"), 500); // đồng bộ với các route khác
    } catch (error) {
      console.error("Logout failed:", error);
      showToast("❌ Logout failed, but redirecting to sign in...", "warning", 3000);
      closeDropdown();
      setTimeout(() => navigate("/signin"), 1000);
    }
  };

  // --- Helpers an toàn ---
  const displayName =
    user?.full_name ||
    // nếu không có full_name thì lấy phần trước @ của email
    (user?.email ? user.email.split("@")[0] : "") ||
    "User";

  const username =
    (user?.email ? user.email.split("@")[0] : "") ||
    (displayName ? displayName.split(" ")[0] : "U");

  const userInitial = username.charAt(0).toUpperCase();

  // role có thể ở dạng string hoặc trong mảng roles[]
  const roleStr = (
    (user as any)?.role ||
    user?.roles?.[0]?.role_code ||
    user?.roles?.[0]?.role_name ||
    "customer"
  )
    .toString()
    .toLowerCase();

  const email = user?.email || "user@example.com";

  const getRoleColor = (role: string) => {
    switch (role) {
      case "admin":
        return "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300";
      case "manager":
        return "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300";
      case "analyst":
        return "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300";
      default:
        return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
    }
  };

  // Debug nhẹ (chỉ hiện ở dev)
  if (import.meta?.env?.DEV) {
    // eslint-disable-next-line no-console
    console.debug("[UserDropdown] user =", user, "role =", roleStr);
  }

  return (
    <div className="relative">
      <button
        onClick={toggleDropdown}
        className="flex items-center text-gray-700 dropdown-toggle dark:text-gray-400"
      >
        <span className="mr-3 overflow-hidden rounded-full h-11 w-11 bg-gray-200 dark:bg-gray-700 flex items-center justify-center">
          {user ? (
            <span className="text-lg font-semibold text-gray-600 dark:text-gray-300">
              {userInitial}
            </span>
          ) : (
            <img src="/images/user/owner.jpg" alt="User" />
          )}
        </span>

        <span className="block mr-1 font-medium text-theme-sm">
          {username}
        </span>

        <svg
          className={`stroke-gray-500 dark:stroke-gray-400 transition-transform duration-200 ${
            isOpen ? "rotate-180" : ""
          }`}
          width="18"
          height="20"
          viewBox="0 0 18 20"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
        >
          <path
            d="M4.3125 8.65625L9 13.3437L13.6875 8.65625"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>

      <Dropdown
        isOpen={isOpen}
        onClose={closeDropdown}
        className="absolute right-0 mt-[17px] flex w-[260px] flex-col rounded-2xl border border-gray-200 bg-white p-3 shadow-theme-lg dark:border-gray-800 dark:bg-gray-dark"
      >
        <div>
          <div className="flex items-center justify-between">
            <span className="block font-medium text-gray-700 text-theme-sm dark:text-gray-400">
              {displayName}
            </span>
            {roleStr && (
              <span className={`px-2 py-1 text-xs rounded-full ${getRoleColor(roleStr)}`}>
                {roleStr}
              </span>
            )}
          </div>
          <span className="mt-0.5 block text-theme-xs text-gray-500 dark:text-gray-400">
            {email}
          </span>
        </div>

        <ul className="flex flex-col gap-1 pt-4 pb-3 border-b border-gray-200 dark:border-gray-800">
          <li>
            <DropdownItem
              tag="a"
              to="/profile"
              onItemClick={closeDropdown}
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              {/* icon */}
               Trang cá nhân
            </DropdownItem>
          </li>
       
          <li>
            <DropdownItem
              tag="a"
              to="/support"
              onItemClick={closeDropdown}
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              {/* icon */}
              Hỗ trợ
            </DropdownItem>
          </li>
        </ul>

        <button
          onClick={handleLogout}
          className="flex items-center gap-3 px-3 py-2 mt-3 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300 w-full text-left"
        >
          {/* icon */}
          Đăng xuất
        </button>
      </Dropdown>
    </div>
  );
}
