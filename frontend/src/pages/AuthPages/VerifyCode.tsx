import PageMeta from "../../components/common/PageMeta";
import AuthLayout from "./AuthPageLayout";
import VerifyCodeForm from "../../components/auth/VerifyCodeForm";

export default function VerifyCode() {
  return (
    <>
      <PageMeta
        title="Verify Code | TailAdmin - Next.js Admin Dashboard Template"
        description="This is Verify Code page for TailAdmin - React.js Tailwind CSS Admin Dashboard Template"
      />
      <AuthLayout>
        <VerifyCodeForm />
      </AuthLayout>
    </>
  );
}