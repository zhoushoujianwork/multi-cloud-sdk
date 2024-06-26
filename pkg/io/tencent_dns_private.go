package io

import (
	"fmt"
	"log"
	"strings"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/spf13/cast"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	privatedns "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/privatedns/v20201028"
	"github.com/xops-infra/multi-cloud-sdk/pkg/model"
)

func (c *tencentClient) DescribePrivateDomainList(profile string, input model.DescribeDomainListRequest) (model.DescribePrivateDomainListResponse, error) {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return model.DescribePrivateDomainListResponse{}, err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewDescribePrivateZoneListRequest()
	if input.DomainKeyword != nil {
		request.Filters = []*privatedns.Filter{
			{
				Name: tea.String("Domain"),
				Values: []*string{
					input.DomainKeyword,
				},
			},
		}
	}

	// 返回的resp是一个DescribePrivateZoneListResponse的实例，与请求对象对应
	response, err := client.DescribePrivateZoneList(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return model.DescribePrivateDomainListResponse{}, fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return model.DescribePrivateDomainListResponse{}, err
	}
	var domains []model.PrivateDomain
	for _, domain := range response.Response.PrivateZoneSet {
		domains = append(domains, model.PrivateDomain{
			DomainId:    tea.String(cast.ToString(domain.ZoneId)),
			Name:        domain.Domain,
			RecordCount: domain.RecordCount,
			VpcSet:      domain.VpcSet,
			Status:      domain.Status,
			Tags:        domain.Tags,
		})
	}
	return model.DescribePrivateDomainListResponse{
		DomainList: domains,
		TotalCount: response.Response.TotalCount,
	}, nil
}

// getDomainIdByname
func (c *tencentClient) getDomainIdByname(profile string, domain string) (string, error) {
	if strings.HasPrefix(domain, "zone-") {
		// 支持直接使用zoneId
		return domain, nil
	}
	resp, err := c.DescribePrivateDomainList(profile, model.DescribeDomainListRequest{
		DomainKeyword: tea.String(domain),
	})
	if err != nil {
		return "", err
	}
	if len(resp.DomainList) != 1 {
		return "", fmt.Errorf("domain not found,or more than one,filter by keyword:%s", domain)
	}
	return *resp.DomainList[0].DomainId, nil
}

// 过滤 keyword通过数据集再次比较的方式实现，官方接口不支持模糊匹配。
func (c *tencentClient) DescribePrivateRecordList(profile string, input model.DescribePrivateRecordListRequest) (model.DescribePrivateRecordListResponse, error) {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return model.DescribePrivateRecordListResponse{}, err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewDescribePrivateZoneRecordListRequest()
	if input.Domain == nil {
		return model.DescribePrivateRecordListResponse{}, fmt.Errorf("domain is required")
	}
	zoneId, err := c.getDomainIdByname(profile, *input.Domain)
	if err != nil {
		return model.DescribePrivateRecordListResponse{}, err
	}
	request.ZoneId = tea.String(zoneId)
	request.Limit = tea.Int64(100) // 默认100

	// 返回的resp是一个DescribePrivateZoneRecordListResponse的实例，与请求对象对应
	response, err := client.DescribePrivateZoneRecordList(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return model.DescribePrivateRecordListResponse{}, fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return model.DescribePrivateRecordListResponse{}, err
	}
	var records []model.Record
	total := 0
	for {
		for _, record := range response.Response.RecordSet {
			total++
			if input.Keyword != nil {
				if !strings.Contains(*record.SubDomain, *input.Keyword) {
					continue
				}
			}
			records = append(records, model.Record{
				RecordId:   tea.String(cast.ToString(record.RecordId)),
				SubDomain:  record.SubDomain,
				RecordType: record.RecordType,
				Value:      record.RecordValue,
				TTL:        tea.Uint64(cast.ToUint64(record.TTL)),
				Status:     record.Status,
				UpdatedOn:  record.UpdatedOn,
			})
		}
		if cast.ToInt(response.Response.TotalCount) == total {
			break
		}
		request.Offset = tea.Int64(cast.ToInt64(len(records)))
		response, err = client.DescribePrivateZoneRecordList(request)
		if err != nil {
			return model.DescribePrivateRecordListResponse{}, err
		}
	}

	return model.DescribePrivateRecordListResponse{
		RecordList: records,
		TotalCount: tea.Int64(cast.ToInt64(len(records))),
	}, nil

}

func (c *tencentClient) DescribePrivateRecordListWithPages(profile string, input model.DescribePrivateDnsRecordListWithPageRequest) (model.ListRecordsPageResponse, error) {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return model.ListRecordsPageResponse{}, err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewDescribePrivateZoneRecordListRequest()
	if input.Domain == nil {
		return model.ListRecordsPageResponse{}, fmt.Errorf("domain is required")
	}
	zoneId, err := c.getDomainIdByname(profile, *input.Domain)
	if err != nil {
		return model.ListRecordsPageResponse{}, err
	}
	request.ZoneId = tea.String(zoneId)
	request.Limit = tea.Int64(100)
	if input.Limit != nil {
		request.Limit = tea.Int64(cast.ToInt64(input.Limit))
	}
	if input.Page != nil {
		request.Offset = tea.Int64(cast.ToInt64(input.Limit) * cast.ToInt64(tea.Int64Value(input.Page)-1))
	}
	// 返回的resp是一个DescribePrivateZoneRecordListResponse的实例，与请求对象对应
	response, err := client.DescribePrivateZoneRecordList(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return model.ListRecordsPageResponse{}, fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return model.ListRecordsPageResponse{}, err
	}
	var records []model.Record
	for _, record := range response.Response.RecordSet {
		records = append(records, model.Record{
			RecordId:   tea.String(cast.ToString(record.RecordId)),
			SubDomain:  record.SubDomain,
			RecordType: record.RecordType,
			Value:      record.RecordValue,
			TTL:        tea.Uint64(cast.ToUint64(record.TTL)),
			Status:     record.Status,
			UpdatedOn:  record.UpdatedOn,
		})
	}
	var nextPage, prePage *int64
	if *request.Limit == int64(len(records)) {
		if input.Page == nil {
			nextPage = tea.Int64(2)
		} else {
			nextPage = tea.Int64(cast.ToInt64(tea.Int64Value(input.Page)) + 1)
		}
	}
	if input.Page != nil && tea.Int64Value(input.Page) > 1 {
		prePage = tea.Int64(cast.ToInt64(tea.Int64Value(input.Page)) - 1)
	}
	return model.ListRecordsPageResponse{
		RecordList: records,
		NextPage:   nextPage,
		PrePage:    prePage,
	}, nil
}

func (c *tencentClient) CreatePrivateRecord(profile string, input model.CreateRecordRequest) (model.CreateRecordResponse, error) {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return model.CreateRecordResponse{}, err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewCreatePrivateZoneRecordRequest()
	request.TTL = tea.Int64(60) // 默认60
	if input.Domain == nil {
		return model.CreateRecordResponse{}, fmt.Errorf("domain is required")
	}
	zoneId, err := c.getDomainIdByname(profile, *input.Domain)
	if err != nil {
		return model.CreateRecordResponse{}, err
	}
	request.ZoneId = tea.String(zoneId)
	if input.RecordType == nil {
		return model.CreateRecordResponse{}, fmt.Errorf("recordtype is required")
	}
	if input.TTL != nil {
		request.TTL = tea.Int64(cast.ToInt64(*input.TTL))
	}
	if input.SubDomain == nil {
		return model.CreateRecordResponse{}, fmt.Errorf("subdomain is required")
	}
	request.SubDomain = input.SubDomain
	if input.Value == nil {
		return model.CreateRecordResponse{}, fmt.Errorf("value is required")
	}
	request.RecordValue = input.Value
	request.RecordType = input.RecordType
	log.Println(tea.Prettify(request))
	// 返回的resp是一个CreatePrivateZoneRecordResponse的实例，与请求对象对应
	response, err := client.CreatePrivateZoneRecord(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return model.CreateRecordResponse{}, fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return model.CreateRecordResponse{}, err
	}
	return model.CreateRecordResponse{
		RecordId: tea.String(cast.ToString(response.Response.RecordId)),
		Meta:     response.Response,
	}, nil
}

func (c *tencentClient) ModifyPrivateRecord(profile string, input model.ModifyRecordRequest) error {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return err
	}
	if input.Domain == nil {
		return fmt.Errorf("domain is required")
	}
	zoneId, err := c.getDomainIdByname(profile, *input.Domain)
	if err != nil {
		return err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewModifyPrivateZoneRecordRequest()
	request.ZoneId = tea.String(zoneId)
	if input.RecordId == nil {
		return fmt.Errorf("RecordId is required")
	}
	request.RecordId = tea.String(cast.ToString(*input.RecordId))

	if input.RecordType == nil {
		return fmt.Errorf("recordtype is required")
	}
	request.RecordType = input.RecordType

	if input.SubDomain == nil {
		return fmt.Errorf("subdomain is required")
	}
	request.SubDomain = input.SubDomain

	if input.Value == nil {
		return fmt.Errorf("value is required")
	}
	request.RecordValue = input.Value

	if input.TTL != nil {
		request.TTL = tea.Int64(cast.ToInt64(*input.TTL))
	}

	if input.Weight != nil {
		request.Weight = tea.Int64(cast.ToInt64(*input.Weight))
	}

	if input.Status != nil {
		return fmt.Errorf("status is not supported for tencent cloud private dns. remove it from input")
	}

	_, err = client.ModifyPrivateZoneRecord(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return err
	}
	return nil

}

func (c *tencentClient) DeletePrivateRecord(profile string, input model.DeletePrivateRecordRequest) error {
	client, err := c.io.GetTencentPrivateDNSClient(profile)
	if err != nil {
		return err
	}
	if input.Domain == nil {
		return fmt.Errorf("domain is required")
	}
	zoneId, err := c.getDomainIdByname(profile, *input.Domain)
	if err != nil {
		return err
	}
	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := privatedns.NewDeletePrivateZoneRecordRequest()
	request.ZoneId = tea.String(zoneId)
	if input.RecordId == nil && input.RecordIds == nil {
		return fmt.Errorf("recordid & RecordIds must have one")
	}
	request.RecordId = input.RecordId
	request.RecordIdSet = input.RecordIds
	_, err = client.DeletePrivateZoneRecord(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return fmt.Errorf("an api error has returned: %s", err.Error())
	}
	if err != nil {
		return err
	}
	return nil
}
