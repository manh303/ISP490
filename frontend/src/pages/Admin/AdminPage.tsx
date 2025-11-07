// import { DSSHeader } from "./components/DSSHeader";
import { DSSHero } from "./components/DSSHero";
import { DSSFeatures } from "./components/DSSFeatures";
import { DSSSystemOverview } from "./components/DSSSystemOverview";
import { DSSCTA } from "./components/DSSCTA";
// import { DSSFooter } from "./components/DSSFooter";

export default function AdminPage() {
  return (
    <div className="min-h-screen bg-white">
      {/* <DSSHeader /> */}
      <DSSHero />
      <DSSFeatures />
      <DSSSystemOverview />
      <DSSCTA />
      {/* <DSSFooter /> */}
    </div>
  );
}
