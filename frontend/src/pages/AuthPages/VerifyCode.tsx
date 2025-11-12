import PageMeta from "../../components/common/PageMeta";
import AuthLayout from "./AuthPageLayout";
import VerifyCodeForm from "../../components/auth/VerifyCodeForm";

export default function VerifyCode() {
  return (
    <>
      <PageMeta
        title="Verify Code | DSS - Next.js Admin Dashboard Template"
        description="This is Verify Code page for DSS - React.js Tailwind CSS Admin Dashboard Template"
      />
      <AuthLayout>
        <VerifyCodeForm />
      </AuthLayout>
    </>
  );
}