import { useModal } from "../../hooks/useModal";
import { Modal } from "../ui/modal";
import Button from "../ui/button/Button";
import Input from "../form/input/InputField";
import Label from "../form/Label";
import { useAuth } from "../../contexts/AuthContext";
import { useState } from "react";
import { useToast } from "../../contexts/ToastContext";

export default function UserMetaCard() {
  const { isOpen, openModal, closeModal } = useModal();
  const { user, updateProfile, refreshUserData } = useAuth();
  const { showToast } = useToast();
  
  // Form state
  const [formData, setFormData] = useState({
    full_name: '',
    phone: '',
    email: ''
  });
  const [isSaving, setIsSaving] = useState(false);

  // Initialize form data when modal opens
  const handleOpenModal = () => {
    setFormData({
      full_name: user?.full_name || '',
      phone: user?.phone || '',
      email: user?.email || ''
    });
    openModal();
  };

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleSave = async () => {
    try {
      setIsSaving(true);
      
      // Only send changed fields
      const updatedFields: { full_name?: string; phone?: string; email?: string } = {};
      
      if (formData.full_name !== user?.full_name) {
        updatedFields.full_name = formData.full_name;
      }
      if (formData.phone !== user?.phone) {
        updatedFields.phone = formData.phone;
      }
      if (formData.email !== user?.email) {
        updatedFields.email = formData.email;
      }

      // Only update if there are changes
      if (Object.keys(updatedFields).length === 0) {
        showToast("No changes to save", "info", 2000);
        closeModal();
        return;
      }

      // Step 1: Update profile
      showToast("💾 Saving profile changes...", "info", 2000);
      await updateProfile(updatedFields);
      
      // Step 2: Refresh profile data from server
      showToast("🔄 Fetching latest profile data...", "info", 1500);
      await refreshUserData();
      
      // Step 3: Success notification
      showToast("✅ Profile updated and refreshed successfully!", "success", 3000);
      closeModal();
      
      console.log('[UserMetaCard] Profile update completed successfully');
    } catch (error: any) {
      console.error('[UserMetaCard] Profile update error:', error);
      showToast(`❌ ${error.message || 'Failed to update profile'}`, "error", 5000);
    } finally {
      setIsSaving(false);
    }
  };
  return (
    <>
      <div className="p-5 border border-gray-200 rounded-2xl dark:border-gray-800 lg:p-6">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-center xl:justify-between">
          <div className="flex flex-col items-center w-full gap-6 xl:flex-row">
            <div className="w-20 h-20 flex items-center justify-center border border-gray-200 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 dark:border-gray-800">
              <span className="text-2xl font-bold text-white">
                {user?.full_name?.charAt(0).toUpperCase() || user?.email?.charAt(0).toUpperCase() || 'U'}
              </span>
            </div>
            <div className="order-3 xl:order-2">
              <h4 className="mb-2 text-lg font-semibold text-center text-gray-800 dark:text-white/90 xl:text-left">
                {user?.full_name || 'User Name'}
              </h4>
              <div className="flex flex-col items-center gap-1 text-center xl:flex-row xl:gap-3 xl:text-left">
                <div className="flex items-center gap-2">
                  <span className={`px-2 py-1 text-xs rounded-full font-medium ${
                    user?.roles?.[0]?.role_code === 'ADMIN' 
                      ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                      : user?.roles?.[0]?.role_code === 'ANALYST'
                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                      : user?.roles?.[0]?.role_code === 'DATAENGINEER'
                      ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
                  }`}>
                    {user?.roles?.[0]?.role_name || 'User'}
                  </span>
                  <span className={`px-2 py-1 text-xs rounded-full font-medium ${
                    user?.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
                  }`}>
                    {user?.status || 'inactive'}
                  </span>
                </div>
                <div className="hidden h-3.5 w-px bg-gray-300 dark:bg-gray-700 xl:block"></div>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  {user?.email || 'user@example.com'}
                </p>
              </div>
              {user?.phone && (
                <div className="mt-2 text-center xl:text-left">
                  <p className="text-sm text-gray-500 dark:text-gray-400">
                    📞 {user.phone}
                  </p>
                </div>
              )}
            </div>
           
          </div>
          <button
            onClick={handleOpenModal}
            className="flex w-full items-center justify-center gap-2 rounded-full border border-gray-300 bg-white px-4 py-3 text-sm font-medium text-gray-700 shadow-theme-xs hover:bg-gray-50 hover:text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-white/[0.03] dark:hover:text-gray-200 lg:inline-flex lg:w-auto"
          >
            <svg
              className="fill-current"
              width="18"
              height="18"
              viewBox="0 0 18 18"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M15.0911 2.78206C14.2125 1.90338 12.7878 1.90338 11.9092 2.78206L4.57524 10.116C4.26682 10.4244 4.0547 10.8158 3.96468 11.2426L3.31231 14.3352C3.25997 14.5833 3.33653 14.841 3.51583 15.0203C3.69512 15.1996 3.95286 15.2761 4.20096 15.2238L7.29355 14.5714C7.72031 14.4814 8.11172 14.2693 8.42013 13.9609L15.7541 6.62695C16.6327 5.74827 16.6327 4.32365 15.7541 3.44497L15.0911 2.78206ZM12.9698 3.84272C13.2627 3.54982 13.7376 3.54982 14.0305 3.84272L14.6934 4.50563C14.9863 4.79852 14.9863 5.2734 14.6934 5.56629L14.044 6.21573L12.3204 4.49215L12.9698 3.84272ZM11.2597 5.55281L5.6359 11.1766C5.53309 11.2794 5.46238 11.4099 5.43238 11.5522L5.01758 13.5185L6.98394 13.1037C7.1262 13.0737 7.25666 13.003 7.35947 12.9002L12.9833 7.27639L11.2597 5.55281Z"
                fill=""
              />
            </svg>
            Edit
          </button>
        </div>
      </div>
      <Modal isOpen={isOpen} onClose={closeModal} className="max-w-[700px] m-4">
        <div className="no-scrollbar relative w-full max-w-[700px] overflow-y-auto rounded-3xl bg-white p-4 dark:bg-gray-900 lg:p-11">
          <div className="px-2 pr-14">
            <h4 className="mb-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              Edit Personal Information
            </h4>
            <p className="mb-6 text-sm text-gray-500 dark:text-gray-400 lg:mb-7">
              Update your details to keep your profile up-to-date.
            </p>
          </div>
          <form className="flex flex-col">
            <div className="custom-scrollbar h-[450px] overflow-y-auto px-2 pb-3">
              {/* <div>
                <h5 className="mb-5 text-lg font-medium text-gray-800 dark:text-white/90 lg:mb-6">
                  Social Links
                </h5>

                <div className="grid grid-cols-1 gap-x-6 gap-y-5 lg:grid-cols-2">
                  <div>
                    <Label>Facebook</Label>
                    <Input
                      type="text"
                      value="https://www.facebook.com/PimjoHQ"
                    />
                  </div>

                  <div>
                    <Label>X.com</Label>
                    <Input type="text" value="https://x.com/PimjoHQ" />
                  </div>

                  <div>
                    <Label>Linkedin</Label>
                    <Input
                      type="text"
                      value="https://www.linkedin.com/company/pimjo"
                    />
                  </div>

                  <div>
                    <Label>Instagram</Label>
                    <Input type="text" value="https://instagram.com/PimjoHQ" />
                  </div>
                </div>
              </div> */}
              <div className="mt-7">
                <h5 className="mb-5 text-lg font-medium text-gray-800 dark:text-white/90 lg:mb-6">
                  Personal Information
                </h5>

                <div className="grid grid-cols-1 gap-x-6 gap-y-5 lg:grid-cols-2">
                  <div className="col-span-2">
                    <Label>Full Name</Label>
                    <Input 
                      type="text" 
                      value={formData.full_name} 
                      onChange={(e) => handleInputChange('full_name', e.target.value)}
                      placeholder="Enter your full name"
                    />
                  </div>

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Email Address</Label>
                    <Input 
                      type="email" 
                      value={formData.email} 
                      onChange={(e) => handleInputChange('email', e.target.value)}
                      placeholder="Enter your email"
                    />
                  </div>

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Phone</Label>
                    <Input 
                      type="tel" 
                      value={formData.phone} 
                      onChange={(e) => handleInputChange('phone', e.target.value)}
                      placeholder="Enter your phone number"
                    />
                  </div>

                  {/* <div className="col-span-2 lg:col-span-1">
                    <Label>User ID</Label>
                    <Input type="text" value={user?.user_id || ''} disabled />
                  </div> */}

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Status</Label>
                    <Input type="text" value={user?.status || ''} disabled />
                  </div>

                  <div className="col-span-2">
                    <Label>Role</Label>
                    <Input type="text" value={user?.roles?.[0]?.role_name || ''} disabled />
                  </div>
                </div>
              </div>
            </div>
            <div className="flex items-center gap-3 px-2 mt-6 lg:justify-end">
              <Button 
                size="sm" 
                variant="outline" 
                onClick={closeModal}
                disabled={isSaving}
              >
                Close
              </Button>
              <Button 
                size="sm" 
                onClick={handleSave}
                disabled={isSaving}
                className={isSaving ? 'opacity-50 cursor-not-allowed' : ''}
              >
                {isSaving ? (
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    Saving...
                  </div>
                ) : (
                  'Save Changes'
                )}
              </Button>
            </div>
          </form>
        </div>
      </Modal>
    </>
  );
}
