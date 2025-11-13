interface Component11FalseProps {
  className?: string;
  text?: string;
}

function Component11False({ className, text = "Login" }: Component11FalseProps) {
  return (
    <div className={className} data-name="Component 1/1/false">
      <div className="flex flex-col font-['Inter:Semi_Bold',_sans-serif] font-semibold justify-center leading-[0] not-italic relative shrink-0 text-[16px] text-center text-nowrap text-white">
        <p className="leading-[24px] whitespace-pre">{text}</p>
      </div>
    </div>
  );
}

function Heading1() {
  return (
    <div className="content-stretch flex flex-col items-center relative shrink-0 w-full" data-name="Heading 1">
      <div className="flex flex-col font-['Inter:Bold',_sans-serif] font-bold justify-center leading-[0] not-italic relative shrink-0 text-[29.297px] text-center text-gray-900 w-full">
        <p className="leading-[36px]">Chào mừng quay lại</p>
      </div>
    </div>
  );
}

function Container() {
  return (
    <div className="content-stretch flex flex-col items-center relative shrink-0 w-full" data-name="Container">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[15px] text-center text-gray-600 w-full">
        <p className="leading-[24px]">Đăng nhập tài khoản của bạn</p>
      </div>
    </div>
  );
}

function Container1() {
  return (
    <div className="absolute content-stretch flex flex-col gap-[8px] items-start left-[32px] right-[32px] top-[32px]" data-name="Container">
      <Heading1 />
      <Container />
    </div>
  );
}

function Container2() {
  return (
    <div className="box-border content-stretch flex flex-col items-start overflow-clip pb-[2px] pt-px px-0 relative shrink-0 w-[348px]" data-name="Container">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[15.25px] text-gray-900 text-nowrap">
        <p className="leading-[normal] whitespace-pre">Admin</p>
      </div>
    </div>
  );
}

function Input() {
  return (
    <div className="absolute bg-white left-0 right-0 rounded-[8px] top-0" data-name="Input">
      <div className="box-border content-stretch flex items-start justify-center overflow-clip pb-[16px] pt-[17px] px-[18px] relative rounded-[inherit] w-full">
        <Container2 />
      </div>
      <div aria-hidden="true" className="absolute border-2 border-gray-200 border-solid inset-0 pointer-events-none rounded-[8px]" />
    </div>
  );
}

function Container3() {
  return (
    <div className="box-border content-stretch flex flex-col items-start overflow-clip pb-[2px] pt-px px-0 relative shrink-0 w-[348px]" data-name="Container">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[15px] text-gray-900 text-nowrap">
        <p className="leading-[normal] whitespace-pre">admin1234@</p>
      </div>
    </div>
  );
}

function Input1() {
  return (
    <div className="absolute bg-white left-0 right-0 rounded-[8px] top-[76px]" data-name="Input">
      <div className="box-border content-stretch flex items-start justify-center overflow-clip pb-[16px] pt-[17px] px-[18px] relative rounded-[inherit] w-full">
        <Container3 />
      </div>
      <div aria-hidden="true" className="absolute border-2 border-gray-200 border-solid inset-0 pointer-events-none rounded-[8px]" />
    </div>
  );
}

function Form() {
  return (
    <div className="absolute h-[200px] left-[32px] right-[32px] top-[132px]" data-name="Form">
      <Component11False className="absolute bg-blue-600 box-border content-stretch flex items-center justify-center left-0 px-0 py-[12px] rounded-[8px] top-[152px] w-[384px]" />
      <Input />
      <Input1 />
    </div>
  );
}

function Component1() {
  return (
    <div className="content-stretch flex items-center justify-center relative shrink-0" data-name="Component 1">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.234px] text-blue-600 text-center text-nowrap">
        <p className="leading-[20px] whitespace-pre">Quên mật khẩu ?</p>
      </div>
    </div>
  );
}

function Component2() {
  return (
    <div className="content-stretch flex items-center justify-center relative shrink-0" data-name="Component 1">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.234px] text-blue-600 text-center text-nowrap">
        <p className="leading-[20px] whitespace-pre">Tạo tài khoản</p>
      </div>
    </div>
  );
}

function Container4() {
  return (
    <div className="content-stretch flex items-start justify-center relative shrink-0 w-full" data-name="Container">
      <div className="flex flex-col font-['Inter:Regular',_sans-serif] font-normal justify-center leading-[0] not-italic relative shrink-0 text-[13.563px] text-center text-gray-600 text-nowrap">
        <p className="leading-[20px] whitespace-pre">{`Don't have an account? `}</p>
      </div>
      <Component2 />
    </div>
  );
}

function Container5() {
  return (
    <div className="absolute box-border content-stretch flex flex-col gap-[17px] items-center left-[32px] pb-0 pt-[3px] px-0 right-[32px] top-[356px]" data-name="Container">
      <Component1 />
      <Container4 />
    </div>
  );
}

export default function BackgroundShadow() {
  return (
    <div className="bg-white overflow-clip relative rounded-[16px] shadow-[0px_20px_25px_-5px_rgba(0,0,0,0.1),0px_8px_10px_-6px_rgba(0,0,0,0.1)] size-full" data-name="Background+Shadow">
      <Container1 />
      <Form />
      <Container5 />
    </div>
  );
}