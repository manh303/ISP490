import React, { useState, useEffect } from 'react';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import DatePicker from 'react-datepicker';
import { FaRegCalendarAlt } from 'react-icons/fa';
import 'react-datepicker/dist/react-datepicker.css';
import { vi } from 'date-fns/locale';
import { getPricePredictionHistory, onlinePricePrediction, PricePredictionHistory, OnlinePricePredictionRequest, OnlinePricePredictionResponse } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import { Table } from '../../components/ui/table';

const PricePredictionPage: React.FC = () => {
  const [history, setHistory] = useState<PricePredictionHistory | null>(null);
  const [onlineResult, setOnlineResult] = useState<OnlinePricePredictionResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [onlineLoading, setOnlineLoading] = useState(false);

  // Form states
  dayjs.extend(utc);
  dayjs.extend(timezone);
  const [historyForm, setHistoryForm] = useState({
    product_key: '',
    platform_code: '',
    from_date: '', // ISO string
    to_date: '',   // ISO string
    model_name: '',
    model_version: ''
  });

  // Date objects for DatePicker
  const [fromDate, setFromDate] = useState<Date | null>(null);
  const [toDate, setToDate] = useState<Date | null>(null);

  const [onlineForm, setOnlineForm] = useState<OnlinePricePredictionRequest>({
    platform_code: '',
    product_key: '',
    current_price: 0,
    avg_rating: 0,
    review_count: 0,
    model_name: 'price_forecast_rf',
    model_version: 'v1.0'
  });

  const handleHistorySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      const data = await getPricePredictionHistory(historyForm);
      setHistory(data);
    } catch (error) {
      console.error('Error fetching price prediction history:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleOnlineSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setOnlineLoading(true);
      const data = await onlinePricePrediction(onlineForm);
      setOnlineResult(data);
    } catch (error) {
      console.error('Error getting online price prediction:', error);
    } finally {
      setOnlineLoading(false);
    }
  };

  const handleHistoryChange = (field: string, value: string) => {
    setHistoryForm(prev => ({ ...prev, [field]: value }));
  };

  const handleOnlineChange = (field: string, value: any) => {
    setOnlineForm(prev => ({ ...prev, [field]: value }));
  };

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' }
  ];

  // Custom input cho DatePicker để đồng bộ UI
  const CustomDateInput = React.forwardRef<HTMLButtonElement, any>(({ value, onClick, placeholder, onChange }, ref) => (
    <button
      type="button"
      onClick={onClick}
      ref={ref}
      className="w-full flex items-center border border-gray-300 rounded-lg px-3 py-2 bg-white hover:border-blue-400 focus:border-blue-500 focus:outline-none transition-colors duration-150 shadow-sm"
      style={{ minHeight: 40 }}
    >
      <span className={`flex-1 text-left ${!value ? 'text-gray-400' : 'text-gray-900'}`}>{value || placeholder}</span>
      <FaRegCalendarAlt className="ml-2 text-blue-500 text-lg" />
    </button>
  ));
  CustomDateInput.displayName = 'CustomDateInput';

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Dự đoán Giá</h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* History Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Lịch sử Dự đoán</h2>

          <Form onSubmit={handleHistorySubmit}>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Mã Sản phẩm *</label>
                <Input
                  type="text"
                  value={historyForm.product_key}
                  onChange={(e) => handleHistoryChange('product_key', e.target.value)}
                  placeholder="vd: tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Mã Nền tảng *</label>
                <Select
                  options={platformOptions}
                  defaultValue={historyForm.platform_code}
                  onChange={(value) => handleHistoryChange('platform_code', value)}
                  placeholder="Chọn nền tảng"
                />
              </div>


              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Từ ngày *</label>
                <DatePicker
                  selected={fromDate}
                  onChange={(date: Date | null) => {
                    setFromDate(date);
                    handleHistoryChange('from_date', date ? dayjs(date).tz('Asia/Ho_Chi_Minh').format('YYYY-MM-DD') : '');
                  }}
                  dateFormat="dd/MM/yyyy"
                  placeholderText="Chọn ngày bắt đầu"
                  maxDate={toDate || undefined}
                  showMonthDropdown
                  showYearDropdown
                  dropdownMode="select"
                  locale={vi}
                  customInput={<CustomDateInput />}
                  popperClassName="z-50"
                  calendarClassName="rounded-lg shadow-lg border border-gray-200"
                  dayClassName={date =>
                    'text-sm rounded-full transition-colors duration-100 ' +
                    (dayjs(date).isSame(fromDate, 'date') ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
                  }
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Đến ngày *</label>
                <DatePicker
                  selected={toDate}
                  onChange={(date: Date | null) => {
                    setToDate(date);
                    handleHistoryChange('to_date', date ? dayjs(date).tz('Asia/Ho_Chi_Minh').format('YYYY-MM-DD') : '');
                  }}
                  dateFormat="dd/MM/yyyy"
                  placeholderText="Chọn ngày kết thúc"
                  minDate={fromDate || undefined}
                  showMonthDropdown
                  showYearDropdown
                  dropdownMode="select"
                  locale={vi}
                  customInput={<CustomDateInput />}
                  popperClassName="z-50"
                  calendarClassName="rounded-lg shadow-lg border border-gray-200"
                  dayClassName={date =>
                    'text-sm rounded-full transition-colors duration-100 ' +
                    (dayjs(date).isSame(toDate, 'date') ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
                  }
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Tên Mô hình</label>
                <Input
                  type="text"
                  value={historyForm.model_name}
                  onChange={(e) => handleHistoryChange('model_name', e.target.value)}
                  placeholder="Tùy chọn"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Phiên bản Mô hình</label>
                <Input
                  type="text"
                  value={historyForm.model_version}
                  onChange={(e) => handleHistoryChange('model_version', e.target.value)}
                  placeholder="Tùy chọn"
                />
              </div>
            </div>

            <Button disabled={loading}>
              {loading ? 'Đang tải...' : 'Lấy Lịch sử'}
            </Button>
          </Form>

          {/* History Results */}
          {history && (
            <div className="mt-6">
              <h3 className="text-lg font-medium mb-3">Lịch sử Dự đoán cho {history.product_key}</h3>
              <div className="overflow-x-auto">
                <Table>
                  <thead>
                    <tr>
                      <th className="px-4 py-2 text-left">Ngày</th>
                      <th className="px-4 py-2 text-left">Giá Dự đoán</th>
                      <th className="px-4 py-2 text-left">CI Thấp</th>
                      <th className="px-4 py-2 text-left">CI Cao</th>
                      <th className="px-4 py-2 text-left">ID Chạy</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.points.map((point, index) => (
                      <tr key={index} className="border-t">
                        <td className="px-4 py-2">{new Date(point.date).toLocaleDateString()}</td>
                        <td className="px-4 py-2">{point.predicted_price.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.ci_lower.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.ci_upper.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.run_id}</td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              </div>
            </div>
          )}
        </div>

        {/* Online Prediction Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Dự đoán Giá Trực tuyến</h2>

          <Form onSubmit={handleOnlineSubmit}>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Mã Nền tảng *</label>
                <Select
                  options={platformOptions}
                  defaultValue={onlineForm.platform_code}
                  onChange={(value) => handleOnlineChange('platform_code', value)}
                  placeholder="Chọn nền tảng"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Mã Sản phẩm *</label>
                <Input
                  type="text"
                  value={onlineForm.product_key}
                  onChange={(e) => handleOnlineChange('product_key', e.target.value)}
                  placeholder="vd: tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Giá Hiện tại *</label>
                <Input
                  type="number"
                  value={onlineForm.current_price}
                  onChange={(e) => handleOnlineChange('current_price', parseFloat(e.target.value))}
                  min="0"
                  step={0.01}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Đánh giá Trung bình *</label>
                <Input
                  type="number"
                  value={onlineForm.avg_rating}
                  onChange={(e) => handleOnlineChange('avg_rating', parseFloat(e.target.value))}
                  min="0"
                  max="5"
                  step={0.1}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Số Lượt Đánh giá *</label>
                <Input
                  type="number"
                  value={onlineForm.review_count}
                  onChange={(e) => handleOnlineChange('review_count', parseInt(e.target.value))}
                  min="0"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Tên Mô hình</label>
                <Input
                  type="text"
                  value={onlineForm.model_name}
                  onChange={(e) => handleOnlineChange('model_name', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Phiên bản Mô hình</label>
                <Input
                  type="text"
                  value={onlineForm.model_version}
                  onChange={(e) => handleOnlineChange('model_version', e.target.value)}
                />
              </div>
            </div>

            <Button disabled={onlineLoading}>
              {onlineLoading ? 'Đang dự đoán...' : 'Dự đoán Giá'}
            </Button>
          </Form>

          {/* Online Prediction Result */}
          {onlineResult && (
            <div className="mt-6 p-4 bg-blue-50 rounded-lg">
              <h3 className="text-lg font-medium mb-3">Kết quả Dự đoán</h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <span className="font-medium">Giá Dự đoán:</span>
                  <div className="text-2xl font-bold text-blue-600">
                    {onlineResult.predicted_price.toLocaleString()} VND
                  </div>
                </div>
                <div>
                  <span className="font-medium">Khoảng Tin cậy:</span>
                  <div className="text-sm">
                    {onlineResult.ci_lower.toLocaleString()} - {onlineResult.ci_upper.toLocaleString()} VND
                  </div>
                </div>
                <div>
                  <span className="font-medium">Mô hình:</span>
                  <div>{onlineResult.model_name} ({onlineResult.model_version})</div>
                </div>
                <div>
                  <span className="font-medium">Độ trễ:</span>
                  <div>{onlineResult.latency_ms}ms</div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default PricePredictionPage;