import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
import { getAllRoles } from "../../services/roleApi";
import { Button } from '../../components/ui/figma/button';
import { Badge } from '../../components/ui/figma/badge';
import { useToast } from "../../contexts/ToastContext";

interface User {
  user_id: number;
  full_name: string;
  email: string;
  phone: string;
  role_code: string;
  role_name: string;
  status: string;
}

interface UserDetailsProps {
  userId: number;
  onBack: () => void;
  editMode?: boolean;
}

export default function UserDetails(props: UserDetailsProps) {
  const { userId, onBack, editMode } = props;
  const { showToast } = useToast();
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [updateError, setUpdateError] = useState<string | null>(null);
  const [editModeState, setEditModeState] = useState<boolean>(!!editMode);
  const [form, setForm] = useState({ full_name: "", phone: "", role: "CUSTOMER" });
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [passwordError, setPasswordError] = useState<string>("");
  const [showRoleDropdown, setShowRoleDropdown] = useState(false);
  const [showPasswordForm, setShowPasswordForm] = useState(false);
  const [availableRoles, setAvailableRoles] = useState<Array<{ role_code: string; role_name: string }>>([]);

  useEffect(() => {
    setLoading(true);
    setError(null);
    userApi.getUser(userId)
      .then((user) => {
        setUser(user);
        setForm({
          full_name: user.full_name || "",
          phone: user.phone || "",
          role: user.role_code || "CUSTOMER"
        });
      })
      .catch((err) => {
        const detail = err?.response?.data?.detail;
        let errorMsg = "Unknown error";
        if (typeof detail === 'string') {
          errorMsg = detail;
        } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
          errorMsg = detail[0].msg;
        } else if (detail?.msg) {
          errorMsg = detail.msg;
        }
        setError(errorMsg);
      })
      .finally(() => setLoading(false));

    // Fetch roles
    getAllRoles({ page: 1, limit: 100 })
      .then((rolesData) => {
        if (rolesData.success) {
          setAvailableRoles(rolesData.data.map(role => ({
            role_code: role.role_code,
            role_name: role.role_name
          })));
        }
      })
      .catch((err) => {
        console.error("Error fetching roles:", err);
      });
  }, [userId]);

  const handleUpdate = async () => {
    setLoading(true);
    setUpdateError(null);
    try {
      await userApi.updateUser(userId, {
        full_name: form.full_name,
        phone: form.phone,
        role: form.role,
        status: user!.status
      });
      // Re-fetch user info after update
      const updatedUser = await userApi.getUser(userId);
      setUser(updatedUser);
      setForm({
        full_name: updatedUser.full_name || "",
        phone: updatedUser.phone || "",
        role: updatedUser.role_code || "CUSTOMER"
      });
      setEditModeState(false);
      showToast('✓ User information updated successfully!', 'success');
    } catch (err: any) {
      console.error(" Error updating user:", err);
      const detail = err?.response?.data?.detail;
      let errorMsg = "Update failed";
      if (typeof detail === 'string') {
        errorMsg = detail;
      } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
        errorMsg = detail[0].msg;
      } else if (detail?.msg) {
        errorMsg = detail.msg;
      }
      setUpdateError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleUpdatePassword = async () => {
    // Reset error
    setPasswordError("");

    // Validation
    if (!password || !confirmPassword) {
      setPasswordError("Please enter both passwords");
      return;
    }

    if (password.length < 6) {
      setPasswordError("Password must be at least 6 characters");
      return;
    }

    if (password !== confirmPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

    setLoading(true);
    try {
      await userApi.updateUserPassword(userId, password);
      setPassword("");
      setConfirmPassword("");
      setPasswordError("");
      setShowPasswordForm(false);
      showToast('✓ Password updated successfully!', 'success');
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      let errorMsg = "Password update failed";
      if (typeof detail === 'string') {
        errorMsg = detail;
      } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
        errorMsg = detail[0].msg;
      } else if (detail?.msg) {
        errorMsg = detail.msg;
      }
      setPasswordError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleClosePasswordModal = () => {
    setShowPasswordForm(false);
    setPassword("");
    setConfirmPassword("");
    setPasswordError("");
  };

  const handleDisable = async () => {
    setLoading(true);
    try {
      await userApi.disableUser(userId);
      showToast('✓ Account disabled successfully!', 'success');
      onBack();
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      let errorMsg = "Disable failed";
      if (typeof detail === 'string') {
        errorMsg = detail;
      } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
        errorMsg = detail[0].msg;
      } else if (detail?.msg) {
        errorMsg = detail.msg;
      }
      setError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleRestore = async () => {
    setLoading(true);
    try {
      await userApi.restoreUser(userId);
      showToast('✓ Account restored successfully!', 'success');
      onBack();
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      let errorMsg = "Restore failed";
      if (typeof detail === 'string') {
        errorMsg = detail;
      } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
        errorMsg = detail[0].msg;
      } else if (detail?.msg) {
        errorMsg = detail.msg;
      }
      setError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  // const handlePermanentDelete = async () => {
  //   setLoading(true);
  //   try {
  //     await userApi.permanentDeleteUser(userId);
  //     onBack();
  //   } catch (err) {
  //     setError("Permanent delete failed");
  //   } finally {
  //     setLoading(false);
  //   }
  // };

  if (loading) return <div className="text-gray-500 text-center py-8">Loading user data...</div>;
  if (error) return <div className="text-red-500 text-center py-8">{error}<button onClick={() => setError(null)} className="ml-4 text-blue-600 underline">Try again</button></div>;
  if (!user) return null;

  // Show password change form
  if (showPasswordForm) {
    return (
      <div className="bg-white rounded-xl shadow-lg border border-gray-200 max-w-lg mx-auto p-8">
        <Button variant="outline" className="mb-6" onClick={handleClosePasswordModal}>
          ← Back
        </Button>
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Change Password</h2>

        <div className="space-y-4">
          {/* User Info Display */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
            <div className="flex items-center gap-3">
              <svg className="w-8 h-8 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
              </svg>
              <div>
                <p className="font-semibold text-gray-900">{user.full_name}</p>
                <p className="text-sm text-gray-600">{user.email}</p>
              </div>
            </div>
          </div>

          {/* Password input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              New Password <span className="text-red-500">*</span>
            </label>
            <input
              className={`w-full border ${passwordError ? 'border-red-400 focus:ring-red-300' : 'border-gray-300 focus:ring-blue-300'} rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 text-base transition-all`}
              type="password"
              value={password}
              onChange={e => {
                setPassword(e.target.value);
                setPasswordError("");
              }}
              placeholder="Enter new password (minimum 6 characters)"
            />
          </div>

          {/* Confirm Password input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Confirm Password <span className="text-red-500">*</span>
            </label>
            <input
              className={`w-full border ${passwordError ? 'border-red-400 focus:ring-red-300' : 'border-gray-300 focus:ring-blue-300'} rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 text-base transition-all`}
              type="password"
              value={confirmPassword}
              onChange={e => {
                setConfirmPassword(e.target.value);
                setPasswordError("");
              }}
              placeholder="Re-enter new password"
            />
          </div>

          {/* Error message */}
          {passwordError && (
            <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
              <span className="text-red-500">⚠️</span>
              <span className="text-sm text-red-600 font-medium">{passwordError}</span>
            </div>
          )}

          {/* Success indicator when passwords match */}
          {password && confirmPassword && password === confirmPassword && password.length >= 6 && (
            <div className="flex items-center gap-2 p-3 bg-green-50 border border-green-200 rounded-lg">
              <span className="text-green-500">✓</span>
              <span className="text-sm text-green-600 font-medium">Passwords match</span>
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-3 mt-6">
            <Button
              variant="outline"
              onClick={handleClosePasswordModal}
              className="flex-1"
              disabled={loading}
            >
              Cancel
            </Button>
            <Button
              variant="default"
              onClick={handleUpdatePassword}
              disabled={loading || !password || !confirmPassword}
              className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-semibold disabled:bg-gray-300 disabled:cursor-not-allowed"
            >
              {loading ? 'Updating...' : 'Update Password'}
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl shadow-lg border border-gray-200 max-w-lg mx-auto p-8">
      <Button variant="outline" className="mb-6" onClick={onBack}>
        ← Back to List
      </Button>
      <h2 className="text-2xl font-bold mb-6 text-gray-800">
        {editModeState ? 'Edit User' : 'User Details'}
      </h2>

      {/* User Information */}
      <div className="mb-8">
        {editModeState ? (
          <div className="space-y-5">
            {/* Update Error Message */}
            {updateError && (
              <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
                <span className="text-red-500">⚠️</span>
                <span className="text-sm text-red-600 font-medium">{updateError}</span>
                <button onClick={() => setUpdateError(null)} className="ml-auto text-red-400 hover:text-red-600">✕</button>
              </div>
            )}
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Full Name</label>
              <input className="w-full border border-gray-300 rounded-lg px-4 py-2 focus:outline-none focus:ring focus:border-brand-500 text-gray-900 text-base" value={form.full_name} onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} placeholder="Enter full name" />
            </div>
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Phone Number</label>
              <input className="w-full border border-gray-300 rounded-lg px-4 py-2 focus:outline-none focus:ring focus:border-brand-500 text-gray-900 text-base" value={form.phone} onChange={e => setForm(f => ({ ...f, phone: e.target.value }))} placeholder="Enter phone number" />
            </div>
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Role</label>
              <div className="relative w-full">
                <button
                  type="button"
                  className="w-full border border-gray-300 bg-white rounded-lg px-4 py-2 text-left flex justify-between items-center focus:outline-none focus:ring-2 focus:ring-blue-500"
                  onClick={() => setShowRoleDropdown(v => !v)}
                >
                  <span className="text-gray-900">
                    {availableRoles.find(opt => opt.role_code === form.role)?.role_name || "Select role"}
                  </span>
                  <span className={`ml-2 text-gray-400 transition-transform ${showRoleDropdown ? 'rotate-180' : ''}`}>▼</span>
                </button>
                {showRoleDropdown && (
                  <ul className="absolute z-10 w-full bg-white border border-gray-200 rounded-lg mt-1 shadow-lg max-h-60 overflow-auto">
                    {availableRoles.map(opt => (
                      <li
                        key={opt.role_code}
                        className={`px-4 py-3 text-base cursor-pointer hover:bg-blue-50 transition-colors ${form.role === opt.role_code ? 'bg-blue-100 font-semibold text-blue-700' : 'text-gray-700'
                          }`}
                        onClick={() => {
                          setForm(f => ({ ...f, role: opt.role_code }));
                          setShowRoleDropdown(false);
                        }}
                      >
                        {opt.role_name}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
            <div className="flex gap-3 mt-2">
              <Button className="flex-1 bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors" onClick={handleUpdate} disabled={loading} variant="default">Save Changes</Button>
              <Button className="flex-1" variant="outline" onClick={() => setEditModeState(false)}>Cancel</Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="bg-gray-50 rounded-lg p-4 space-y-3">
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Full Name:</span>
                <span className="text-gray-900">{user.full_name}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Email:</span>
                <span className="text-gray-900">{user.email}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Phone Number:</span>
                <span className="text-gray-900">{user.phone || 'Not updated'}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Role:</span>
                <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'} className="text-sm">
                  {user.role_name}
                </Badge>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Status:</span>
                {user.status === 'active' ? (
                  <Badge variant="default" className="bg-green-500 text-white hover:bg-green-600">
                    ✓ Active
                  </Badge>
                ) : (
                  <Badge variant="destructive" className="bg-gray-500 text-white">
                    ✗ Disabled
                  </Badge>
                )}
              </div>
            </div>

            {/* Always show action buttons in view mode */}
            <div className="flex gap-3 mt-4">
              <Button className="flex-1" variant="outline" onClick={() => setEditModeState(true)}>
                <span className="font-semibold">✏️ Edit Information</span>
              </Button>
              <Button className="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors" variant="default" onClick={() => setShowPasswordForm(true)}>
                <span className="font-semibold">🔒 Change Password</span>
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Change Password - Only show in edit mode */}
      {editModeState && editMode !== false && (
        <Button
          className="w-full mb-8 bg-blue-600 hover:bg-blue-700 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
          onClick={() => setShowPasswordForm(true)}
        >
          Change Password
        </Button>
      )}

      {/* Administrative Actions */}
      <div className="flex flex-col gap-3">
        {user.status === 'active' ? (
          <Button
            variant="destructive"
            onClick={handleDisable}
            disabled={loading}
            className={`font-semibold py-2 bg-red-600 hover:bg-red-700 text-white border-none ${loading ? 'opacity-60 cursor-not-allowed' : ''}`}
          >
            🚫 Disable Account
          </Button>
        ) : (
          <Button
            variant="outline"
            onClick={handleRestore}
            disabled={loading}
            className={`font-semibold py-2 ${loading ? 'bg-gray-200 text-gray-500 border border-gray-300 cursor-not-allowed' : ''}`}
          >
            ✓ Restore Account
          </Button>
        )}
      </div>
    </div>
  );
}
