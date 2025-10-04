import PageMeta from "../../components/common/PageMeta";
import AuthLayout from "./AuthPageLayout";
import ResetPasswordForm from "../../components/auth/ResetPasswordForm";

export default function ResetPassword() {
  return (
    <>
      <PageMeta
        title="Reset Password | TailAdmin - Next.js Admin Dashboard Template"
        description="This is Reset Password page for TailAdmin - React.js Tailwind CSS Admin Dashboard Template"
      />
      <AuthLayout>
        <ResetPasswordForm />
      </AuthLayout>
    </>
  );
}