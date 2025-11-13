// Example usage of Role API functions
// This file demonstrates how to use each API function independently

import React from 'react';
import {
  getAllRoles,
  createRole,
  getRoleDetails,
  updateRole,
  deleteRole,
  activateRole,
  deactivateRole,
  getRoleUsers,
  type GetAllRolesParams,
  type CreateRoleData,
  type UpdateRoleData,
  type Role,
} from '../services/roleApi';

// ============================================================================
// EXAMPLE 1: Get All Roles (with pagination and filtering)
// ============================================================================
export async function exampleGetAllRoles() {
  try {
    // Basic usage - get first page with default limit
    const allRoles = await getAllRoles();
    console.log('All roles:', allRoles);
    
    // With pagination
    const page2 = await getAllRoles({ page: 2, limit: 10 });
    console.log('Page 2 roles:', page2);
    
    // Get only active roles
    const activeRoles = await getAllRoles({ page: 1, limit: 20, active_only: true });
    console.log('Active roles:', activeRoles);
    
    // Response structure:
    // {
    //   success: true,
    //   data: [...roles],
    //   total: 5,
    //   page: 1,
    //   limit: 20
    // }
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
  }
}

// ============================================================================
// EXAMPLE 2: Create New Role
// ============================================================================
export async function exampleCreateRole() {
  try {
    const newRoleData: CreateRoleData = {
      role_code: 'PROJECT_MANAGER',
      role_name: 'Project Manager',
      description: 'Manages projects and coordinates team activities'
    };
    
    const response = await createRole(newRoleData);
    console.log('Created role:', response);
    // Response: { success: true, message: "...", role_id: 6 }
    
    return response.role_id; // Return the new role ID
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 3: Get Role Details
// ============================================================================
export async function exampleGetRoleDetails(roleId: number) {
  try {
    const roleDetails = await getRoleDetails(roleId);
    console.log('Role details:', roleDetails);
    
    // Response includes:
    // - role_id, role_code, role_name, description, is_active
    // - permissions (optional array)
    // - modules (optional array)
    // - actions (optional array)
    // - admin_features (optional object)
    // - user_count (optional number)
    
    console.log('Role name:', roleDetails.role_name);
    console.log('Is active:', roleDetails.is_active);
    console.log('User count:', roleDetails.user_count);
    console.log('Permissions:', roleDetails.permissions);
    
    return roleDetails;
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 4: Update Role
// ============================================================================
export async function exampleUpdateRole(roleId: number) {
  try {
    const updateData: UpdateRoleData = {
      role_name: 'Senior Project Manager',
      description: 'Senior-level project manager with extensive experience'
    };
    
    const response = await updateRole(roleId, updateData);
    console.log('Updated role:', response);
    // Response: { success: true, message: "...", role_id: 6 }
    
    // Note: You can update only one field if needed
    await updateRole(roleId, { role_name: 'New Name' });
    // or
    await updateRole(roleId, { description: 'New description' });
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 5: Delete Role
// ============================================================================
export async function exampleDeleteRole(roleId: number) {
  try {
    const response = await deleteRole(roleId);
    console.log('Deleted role:', response);
    // Response: { success: true, message: "...", role_id: 6 }
    
    // Note: Can only delete if no users are assigned to this role
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    // Common error: "Cannot delete role with assigned users"
    throw error;
  }
}

// ============================================================================
// EXAMPLE 6: Deactivate Role
// ============================================================================
export async function exampleDeactivateRole(roleId: number) {
  try {
    const response = await deactivateRole(roleId);
    console.log('Deactivated role:', response);
    // Response: { success: true, message: "...", role_id: 6 }
    
    // Users keep their role assignment, but the role becomes inactive
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 7: Activate Role
// ============================================================================
export async function exampleActivateRole(roleId: number) {
  try {
    const response = await activateRole(roleId);
    console.log('Activated role:', response);
    // Response: { success: true, message: "...", role_id: 6 }
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 8: Get Users Assigned to Role
// ============================================================================
export async function exampleGetRoleUsers(roleId: number) {
  try {
    // Basic usage
    const users = await getRoleUsers(roleId);
    console.log('Users with this role:', users);
    
    // With pagination
    const page2Users = await getRoleUsers(roleId, { page: 2, limit: 10 });
    console.log('Page 2 users:', page2Users);
    
    // Note: Response structure may vary, handle accordingly
    // Could be: string, array, or object with { success, data, total, page, limit }
    
  } catch (error: any) {
    console.error('Error:', error?.response?.data?.detail);
    throw error;
  }
}

// ============================================================================
// EXAMPLE 9: Complete Workflow - Create, Update, and Manage Role
// ============================================================================
export async function exampleCompleteWorkflow() {
  try {
    // Step 1: Create a new role
    console.log('Step 1: Creating role...');
    const createResponse = await createRole({
      role_code: 'TEAM_LEAD',
      role_name: 'Team Lead',
      description: 'Leads a team of developers'
    });
    const newRoleId = createResponse.role_id;
    console.log('Created role with ID:', newRoleId);
    
    // Step 2: Get role details
    console.log('\nStep 2: Fetching role details...');
    const roleDetails = await getRoleDetails(newRoleId);
    console.log('Role details:', roleDetails);
    
    // Step 3: Update the role
    console.log('\nStep 3: Updating role...');
    await updateRole(newRoleId, {
      role_name: 'Senior Team Lead',
      description: 'Senior team lead with 5+ years experience'
    });
    console.log('Role updated successfully');
    
    // Step 4: Check if any users have this role
    console.log('\nStep 4: Checking users...');
    const users = await getRoleUsers(newRoleId);
    console.log('Users with this role:', users);
    
    // Step 5: Deactivate the role (if needed)
    console.log('\nStep 5: Deactivating role...');
    await deactivateRole(newRoleId);
    console.log('Role deactivated');
    
    // Step 6: Reactivate the role
    console.log('\nStep 6: Reactivating role...');
    await activateRole(newRoleId);
    console.log('Role reactivated');
    
    // Step 7: Try to delete (will fail if users are assigned)
    console.log('\nStep 7: Attempting to delete...');
    await deleteRole(newRoleId);
    console.log('Role deleted successfully');
    
  } catch (error: any) {
    console.error('Workflow error:', error?.response?.data?.detail);
  }
}

// ============================================================================
// EXAMPLE 10: Error Handling
// ============================================================================
export async function exampleErrorHandling() {
  // Example 1: Handle validation errors
  try {
    await createRole({
      role_code: 'invalid code', // Invalid format (should be uppercase)
      role_name: '',
      description: ''
    });
  } catch (error: any) {
    if (error?.response?.status === 422) {
      console.error('Validation error:', error.response.data.detail);
    }
  }
  
  // Example 2: Handle not found errors
  try {
    await getRoleDetails(99999); // Non-existent role
  } catch (error: any) {
    if (error?.response?.status === 404) {
      console.error('Role not found');
    }
  }
  
  // Example 3: Handle permission errors
  try {
    await deleteRole(1); // May not have permission
  } catch (error: any) {
    if (error?.response?.status === 403) {
      console.error('Permission denied');
    }
  }
  
  // Example 4: Handle conflict errors
  try {
    await deleteRole(1); // Admin role with users
  } catch (error: any) {
    if (error?.response?.status === 409) {
      console.error('Conflict: Cannot delete role with assigned users');
    }
  }
}

// ============================================================================
// EXAMPLE 11: Using with React Hooks
// ============================================================================
export function ExampleReactComponent() {
  const [roles, setRoles] = React.useState<Role[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  
  React.useEffect(() => {
    const fetchRoles = async () => {
      setLoading(true);
      try {
        const data = await getAllRoles({ page: 1, limit: 20 });
        setRoles(data.data);
      } catch (err: any) {
        setError(err?.response?.data?.detail || 'Failed to fetch roles');
      } finally {
        setLoading(false);
      }
    };
    
    fetchRoles();
  }, []);
  
  const handleCreateRole = async () => {
    try {
      await createRole({
        role_code: 'NEW_ROLE',
        role_name: 'New Role',
        description: 'Description'
      });
      // Refresh the list
      const data = await getAllRoles();
      setRoles(data.data);
    } catch (err: any) {
      setError(err?.response?.data?.detail);
    }
  };
  
  // Component JSX...
}

// ============================================================================
// EXAMPLE 12: Batch Operations
// ============================================================================
export async function exampleBatchOperations() {
  try {
    // Get all roles
    const { data: allRoles } = await getAllRoles({ page: 1, limit: 100 });
    
    // Filter inactive roles
    const inactiveRoles = allRoles.filter(role => !role.is_active);
    console.log('Inactive roles:', inactiveRoles.length);
    
    // Activate all inactive roles
    const activatePromises = inactiveRoles.map(role => 
      activateRole(role.role_id).catch(err => ({
        role_id: role.role_id,
        error: err?.response?.data?.detail
      }))
    );
    
    const results = await Promise.all(activatePromises);
    console.log('Activation results:', results);
    
  } catch (error: any) {
    console.error('Batch operation error:', error);
  }
}

// Export all examples
export default {
  exampleGetAllRoles,
  exampleCreateRole,
  exampleGetRoleDetails,
  exampleUpdateRole,
  exampleDeleteRole,
  exampleDeactivateRole,
  exampleActivateRole,
  exampleGetRoleUsers,
  exampleCompleteWorkflow,
  exampleErrorHandling,
  exampleBatchOperations,
};
