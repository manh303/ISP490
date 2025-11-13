import { useModal } from "../../hooks/useModal";
import { Modal } from "../ui/modal";
import Button from "../ui/button/Button";
import Input from "../form/input/InputField";
import Label from "../form/Label";
import { useAuth } from "../../contexts/AuthContext";
import { useState } from "react"; 

export default function UserInfoCard() {
  const { isOpen, openModal, closeModal } = useModal();
  const { user } = useAuth();
  const [showAllPermissions, setShowAllPermissions] = useState(false);
  const [showRoleDetails, setShowRoleDetails] = useState(false);

  const handleSave = () => {
    // Handle save logic here
    console.log("Saving changes...");
    closeModal();
  };
  return (
    <div className="p-5 border border-gray-200 rounded-2xl dark:border-gray-800 lg:p-6">
      <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-3 lg:mb-6">
            <h4 className="text-lg font-semibold text-gray-800 dark:text-white/90">
              Personal Information
            </h4>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              👤 Profile Overview
            </span>
          </div>

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 lg:gap-7 2xl:gap-x-32">
            <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                Full Name
              </p>
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                {user?.full_name || 'N/A'}
              </p>
            </div>

            {/* <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                User ID
              </p>
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                {user?.user_id || 'N/A'}
              </p>
            </div> */}

            <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                Email Address
              </p>
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                {user?.email || 'N/A'}
              </p>
            </div>

            <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                Phone Number
              </p>
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                {user?.phone || 'N/A'}
              </p>
            </div>

            <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                Account Status
              </p>
              <div className="flex items-center gap-2">
                <span className={`flex items-center gap-1 px-2 py-1 text-xs rounded-full font-medium ${
                  user?.status === 'active' 
                    ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                    : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                }`}>
                  {user?.status === 'active' ? '🟢' : '🔴'}
                  {user?.status === 'active' ? 'Active' : 'Inactive'}
                </span>
              </div>
            </div>

            <div>
              <p className="mb-2 text-xs leading-normal text-gray-500 dark:text-gray-400">
                User Role
              </p>
              <div className="flex items-center gap-2">
                <span className={`flex items-center gap-1 px-2 py-1 text-xs rounded-full font-medium ${
                  user?.roles?.[0]?.role_code === 'ADMIN' 
                    ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                    : user?.roles?.[0]?.role_code === 'ANALYST'
                    ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                    : user?.roles?.[0]?.role_code === 'DATAENGINEER'
                    ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
                    : 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
                }`}>
                  {user?.roles?.[0]?.role_code === 'ADMIN' ? '👑' : 
                   user?.roles?.[0]?.role_code === 'ANALYST' ? '📊' : 
                   user?.roles?.[0]?.role_code === 'DATAENGINEER' ? '⚙️' : '👤'}
                  {user?.roles?.[0]?.role_name || 'User'}
                </span>
              </div>
            </div>
          </div>

          {/* Role Details Section - Collapsible */}
          {user?.roles && user.roles.length > 0 && (
            <div className="mt-8">
              <div className="flex items-center justify-between mb-4">
                <h5 className="text-base font-medium text-gray-800 dark:text-white/90">
                  Role Information
                </h5>
                <button
                  onClick={() => setShowRoleDetails(!showRoleDetails)}
                  className="flex items-center gap-1 text-sm text-blue-600 hover:text-blue-700 dark:text-blue-400"
                >
                  {showRoleDetails ? 'Hide Details' : 'View Details'}
                  <svg 
                    className={`w-4 h-4 transition-transform ${showRoleDetails ? 'rotate-180' : ''}`}
                    fill="none" 
                    stroke="currentColor" 
                    viewBox="0 0 24 24"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
              </div>
              
              {showRoleDetails && (
                <div className="p-4 bg-gray-50 rounded-lg dark:bg-gray-800 transition-all duration-300">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div>
                      <p className="text-xs text-gray-500 dark:text-gray-400">Role Code</p>
                      <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                        {user.roles[0].role_code}
                      </p>
                    </div>
                    {/* <div>
                      <p className="text-xs text-gray-500 dark:text-gray-400">Role ID</p>
                      <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                        {user.roles[0].role_id}
                      </p>
                    </div> */}
                    <div>
                      <p className="text-xs text-gray-500 dark:text-gray-400">Description</p>
                      <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                        {user.roles[0].description || 'System role with administrative privileges'}
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Permissions Summary - Friendly Display */}
          {user?.permissions && user.permissions.length > 0 && (
            <div className="mt-8">
              <div className="flex items-center justify-between mb-4">
                <h5 className="text-base font-medium text-gray-800 dark:text-white/90">
                  Access Permissions
                </h5>
                <div className="flex items-center gap-2">
                  <span className="px-2 py-1 text-xs bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300 rounded-full">
                    {user.permissions.length} permissions
                  </span>
                  <button
                    onClick={() => setShowAllPermissions(!showAllPermissions)}
                    className="text-sm text-blue-600 hover:text-blue-700 dark:text-blue-400"
                  >
                    {showAllPermissions ? 'Show Less' : 'View All'}
                  </button>
                </div>
              </div>

              {/* Permission Summary Cards */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                {['system', 'user', 'data', 'analytics'].map((module) => {
                  const modulePerms = user.permissions?.filter(p => p.module === module) || [];
                  return (
                    <div key={module} className="p-3 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-700 rounded-lg">
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-medium text-gray-600 dark:text-gray-300 capitalize">
                          {module}
                        </span>
                        <span className="text-xs bg-blue-500 text-white px-1.5 py-0.5 rounded-full">
                          {modulePerms.length}
                        </span>
                      </div>
                      <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                        {modulePerms.length > 0 ? 'Access granted' : 'No access'}
                      </p>
                    </div>
                  );
                })}
              </div>

              {/* Detailed Permissions - Collapsible */}
              {showAllPermissions && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 transition-all duration-300">
                  {user.permissions.map((permission) => (
                    <div
                      key={permission.perm_id}
                      className="flex items-center justify-between p-3 bg-white border border-gray-200 rounded-lg dark:bg-gray-800 dark:border-gray-700 hover:shadow-sm transition-shadow"
                    >
                      <div className="flex-1">
                        <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                          {permission.perm_name}
                        </p>
                        <p className="text-xs text-gray-500 dark:text-gray-400">
                          Can {permission.action} {permission.module} resources
                        </p>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <span className="px-2 py-0.5 text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300 rounded">
                          {permission.module}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>

        {/* <button
          onClick={openModal}
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
        </button> */}
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
                    <Input type="text" value={user?.full_name || ''} />
                  </div>

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Email Address</Label>
                    <Input 
                      type="email" 
                      value={user?.email || ''} 
                      disabled 
                      className="bg-gray-100 dark:bg-gray-800 cursor-not-allowed"
                    />
                  </div>

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Phone Number</Label>
                    <Input type="tel" value={user?.phone || ''} />
                  </div>

                  {/* <div className="col-span-2 lg:col-span-1">
                    <Label>User ID</Label>
                    <Input 
                      type="text" 
                      value={user?.user_id || ''} 
                      disabled 
                      className="bg-gray-100 dark:bg-gray-800 cursor-not-allowed"
                    />
                  </div> */}

                  <div className="col-span-2 lg:col-span-1">
                    <Label>Account Status</Label>
                    <Input 
                      type="text" 
                      value={user?.status || ''} 
                      disabled 
                      className="bg-gray-100 dark:bg-gray-800 cursor-not-allowed"
                    />
                  </div>

                  <div className="col-span-2">
                    <Label>Current Role</Label>
                    <Input 
                      type="text" 
                      value={`${user?.roles?.[0]?.role_name || 'No Role'} (${user?.roles?.[0]?.role_code || 'N/A'})`} 
                      disabled 
                      className="bg-gray-100 dark:bg-gray-800 cursor-not-allowed"
                    />
                  </div>
                </div>

                {/* Permissions Summary in Edit Modal */}
                {user?.permissions && user.permissions.length > 0 && (
                  <div className="mt-7">
                    <h5 className="mb-5 text-lg font-medium text-gray-800 dark:text-white/90 lg:mb-6">
                      System Permissions ({user.permissions.length})
                    </h5>
                    <div className="max-h-40 overflow-y-auto">
                      <div className="grid grid-cols-1 gap-2">
                        {user.permissions.map((permission) => (
                          <div
                            key={permission.perm_id}
                            className="flex items-center justify-between p-2 bg-gray-50 rounded dark:bg-gray-800"
                          >
                            <div className="flex-1">
                              <p className="text-xs font-medium text-gray-800 dark:text-white/90">
                                {permission.perm_name}
                              </p>
                              <p className="text-xs text-gray-500 dark:text-gray-400">
                                {permission.perm_code}
                              </p>
                            </div>
                            <div className="flex gap-1">
                              <span className="px-1.5 py-0.5 text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300 rounded">
                                {permission.module}
                              </span>
                              <span className="px-1.5 py-0.5 text-xs bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300 rounded">
                                {permission.action}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
            <div className="flex items-center gap-3 px-2 mt-6 lg:justify-end">
              <Button size="sm" variant="outline" onClick={closeModal}>
                Close
              </Button>
              <Button size="sm" onClick={handleSave}>
                Save Changes
              </Button>
            </div>
          </form>
        </div>
      </Modal>
    </div>
  );
}
